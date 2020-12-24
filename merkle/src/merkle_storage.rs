//! # MerkleStorage
//!
//! Storage for key/values with git-like semantics and history.
//!
//! # Data Structure
//! A storage with just one key `a/b/c` and its corresponding value `8` is represented like this:
//!
//! ``
//! [commit] ---> [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//! ``
//!
//! The db then contains the following:
//! ```no_compile
//! <hash_of_blob; blob8>
//! <hash_of_tree3, tree3>, where tree3 is a map {c: hash_blob8}
//! <hash_of_tree2, tree2>, where tree2 is a map {b: hash_of_tree3}
//! <hash_of_tree2, tree2>, where tree1 is a map {a: hash_of_tree2}
//! <hash_of_commit>; commit>, where commit points to the root tree (tree1)
//! ```
//!
//! Then, when looking for a path a/b/c in a spcific commit, we first get the hash of the root tree
//! from the commit, then get the tree from the database, get the hash of "a", look it up in the db,
//! get the hash of "b" from that tree, load from db, then get the hash of "c" and retrieve the
//! final value.
//!
//!
//! Now, let's assume we want to add a path `X` also referencing the value `8`. That creates a new
//! tree that reuses the previous subtree for `a/b/c` and branches away from root for `X`:
//!
//! ```no_compile
//! [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//!                   ^                             ^
//!                   |                             |
//! [tree_X]----a-----                              |
//!     |                                           |
//!      ----------------------X--------------------
//! ```
//!
//! The following is added to the database:
//! ``
//! <hash_of_tree_X; tree_X>, where tree_X is a map {a: hash_of_tree2, X: hash_of_blob8}
//! ``
//!
//! Reference: https://git-scm.com/book/en/v2/Git-Internals-Git-Objects
use std::hash::Hash;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use im::{OrdMap, HashSet};
use failure::Fail;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use crate::hash::HashType;
use std::convert::TryInto;
use sodiumoxide::crypto::generichash::State;
use crate::codec::{BincodeEncoded, SchemaError};
use crate::schema::KeyValueSchema;
use crate::database::{KeyValueStoreWithSchema, Batch, DB, DBStats, IteratorMode};
use crate::database::DBError;
use failure::_core::fmt::Formatter;
use std::collections::hash_map::RandomState;
use linked_hash_set::LinkedHashSet;
use dashmap::DashMap;

const HASH_LEN: usize = 32;

pub type ContextKey = Vec<String>;
pub type ContextValue = Vec<u8>;
pub type EntryHash = [u8; HASH_LEN];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
enum NodeKind {
    NonLeaf,
    Leaf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Node {
    node_kind: NodeKind,
    entry_hash: EntryHash,
}

type Tree = OrdMap<String, Node>;

#[derive(Debug, Hash, Clone, Serialize, Deserialize)]
struct Commit {
    parent_commit_hash: Option<EntryHash>,
    root_hash: EntryHash,
    time: u64,
    author: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Entry {
    Tree(Tree),
    Blob(ContextValue),
    Commit(Commit),
}

pub type MerkleStorageKV = dyn KeyValueStoreWithSchema<MerkleStorage> + Sync + Send;

struct GC {
    db: Arc<DashMap<EntryHash, Vec<u8>>>,
    blocks: HashMap<EntryHash, usize>
}

impl GC {
    fn new(db: Arc<DashMap<EntryHash, Vec<u8>>>) -> GC {
        GC {
            blocks: Default::default(),
            db,
        }
    }

    fn update(&mut self, e: EntryHash) {
        let mut counter = self.blocks.entry(e).or_insert(0);
        *counter += 1;
    }

    fn delete(&mut self, e: &EntryHash) {
        let mut counter = self.blocks.entry(e.clone()).or_default();
        if *counter <= 0 {
            return;
        }
        *counter -= 1;
    }

    fn clean(&mut self) {
        for (k,v) in &self.blocks {


            if *v <= 0 {
                self.db.remove(k);
            }
        }
    }
}

impl std::fmt::Display for GC {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut display = String::new();
        self.blocks.iter().for_each(|(k, v)| {
            display.push_str(&format!("{:?} : {} \n", k, v));
        }
        );

        write!(f, "{}", display)
    }
}

pub struct MerkleStorage {
    current_stage_tree: Option<Tree>,
    db: Arc<DashMap<EntryHash, Vec<u8>>>,
    // Track commits
    commits: LinkedHashSet<EntryHash>,
    staged: HashMap<EntryHash, Entry>,
    last_commit: Option<Commit>,
    map_stats: MerkleMapStats,
    cumul_set_exec_time: f64,
    gc: GC,
    // divide this by the next field to get avg time spent in _set
    set_exec_times: u64,
    set_exec_times_to_discard: u64, // first N measurements to discard
}

#[derive(Debug, Fail)]
pub enum MerkleError {
    /// External libs errors
    #[fail(display = "Serialization error: {:?}", error)]
    SerializationError { error: bincode::Error },
    #[fail(display = "SledDB error: {:?}", error)]
    DBError { error: DBError },
    /// Internal unrecoverable bugs that should never occur
    #[fail(display = "No root retrieved for this commit!")]
    CommitRootNotFound,
    #[fail(display = "Cannot commit without a predecessor!")]
    MissingAncestorCommit,
    #[fail(display = "There is a commit or three under key {:?}, but not a value!", key)]
    ValueIsNotABlob { key: String },
    #[fail(display = "Found wrong structure. Was looking for {}, but found {}", sought, found)]
    FoundUnexpectedStructure { sought: String, found: String },
    #[fail(display = "Entry not found! Hash={}", hash)]
    EntryNotFound { hash: String },

    /// Wrong user input errors
    #[fail(display = "No value under key {:?}.", key)]
    ValueNotFound { key: String },
    #[fail(display = "Cannot search for an empty key.")]
    KeyEmpty,
}

impl From<DBError> for MerkleError {
    fn from(error: DBError) -> Self { MerkleError::DBError { error } }
}


impl From<bincode::Error> for MerkleError {
    fn from(error: bincode::Error) -> Self { MerkleError::SerializationError { error } }
}

#[derive(Serialize, Debug, Clone, Copy)]
pub struct MerkleMapStats {
    staged_area_elems: u64,
    current_tree_elems: u64,
}

#[derive(Serialize, Debug, Clone, Copy)]
pub struct MerklePerfStats {
    pub avg_set_exec_time_ns: f64,
}

#[derive(Serialize, Debug, Clone)]
pub struct MerkleStorageStats {
    pub db_stats: DBStats,
    pub map_stats: MerkleMapStats,
    pub perf_stats: MerklePerfStats,
}

impl BincodeEncoded for EntryHash {}

impl KeyValueSchema for MerkleStorage {
    type Key = EntryHash;
    type Value = Vec<u8>;

    #[inline]
    fn name() -> &'static str {
        "merkle_storage"
    }
}

impl MerkleStorage {
    pub fn new(db: Arc<DashMap<EntryHash, Vec<u8>>>) -> Self {
        MerkleStorage {
            db: db.clone(),
            commits: LinkedHashSet::new(),
            gc: GC::new(db),
            staged: HashMap::new(),
            current_stage_tree: None,
            last_commit: None,
            map_stats: MerkleMapStats { staged_area_elems: 0, current_tree_elems: 0 },
            cumul_set_exec_time: 0.0,
            set_exec_times: 0,
            set_exec_times_to_discard: 20,
        }
    }

    /// Get value. Staging area is checked first, then last (checked out) commit.
    pub fn get(&mut self, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let root = &self.get_staged_root()?;
        let root_hash = self.hash_tree(&root);

        self.get_from_tree(&root_hash, key)
    }

    /// Get value. Staging area is checked first, then last (checked out) commit.
    pub fn get_by_prefix(&mut self, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let root = self.get_staged_root()?;
        self._get_key_values_by_prefix(root, prefix)
    }

    /// Get value from historical context identified by commit hash.
    pub fn get_history(&self, commit_hash: &EntryHash, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let commit = self.get_commit(commit_hash)?;

        self.get_from_tree(&commit.root_hash, key)
    }

    fn get_from_tree(&self, root_hash: &EntryHash, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let mut full_path = key.clone();
        let file = full_path.pop().ok_or(MerkleError::KeyEmpty)?;
        let path = full_path;
        let root = self.get_tree(root_hash)?;
        let node = self.find_tree(&root, &path)?;

        let node = match node.get(&file) {
            None => return Err(MerkleError::ValueNotFound { key: self.key_to_string(key) }),
            Some(entry) => entry,
        };
        match self.get_entry(&node.entry_hash)? {
            Entry::Blob(blob) => Ok(blob),
            _ => Err(MerkleError::ValueIsNotABlob { key: self.key_to_string(key) })
        }
    }

    // TODO: recursion is risky (stack overflow) and inefficient, try to do it iteratively..
    fn get_key_values_from_tree_recursively(&self, path: &str, entry: &Entry, entries: &mut Vec<(ContextKey, ContextValue)>) -> Result<(), MerkleError> {
        match entry {
            Entry::Blob(blob) => {
                // push key-value pair
                entries.push((self.string_to_key(path), blob.to_vec()));
                Ok(())
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter().map(|(key, child_node)| {
                    let fullpath = path.to_owned() + "/" + key;
                    match self.get_entry(&child_node.entry_hash) {
                        Err(_) => Ok(()),
                        Ok(entry) => self.get_key_values_from_tree_recursively(&fullpath, &entry, entries),
                    }
                }).find_map(|res| {
                    match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    }
                }).unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => {
                match self.get_entry(&commit.root_hash) {
                    Err(err) => Err(err),
                    Ok(entry) => self.get_key_values_from_tree_recursively(path, &entry, entries),
                }
            }
        }
    }

    pub fn get_key_values_by_prefix(&self, context_hash: &EntryHash, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let commit = self.get_commit(context_hash)?;
        let root_tree = self.get_tree(&commit.root_hash)?;
        self._get_key_values_by_prefix(root_tree, prefix)
    }

    fn _get_key_values_by_prefix(&self, root_tree: Tree, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let prefixed_tree = self.find_tree(&root_tree, prefix)?;
        let mut keyvalues: Vec<(ContextKey, ContextValue)> = Vec::new();

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.get_entry(&child_node.entry_hash)?;
            let delimiter: &str;
            if prefix.is_empty() {
                delimiter = "";
            } else {
                delimiter = "/";
            }
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            self.get_key_values_from_tree_recursively(&fullpath, &entry, &mut keyvalues)?;
        }

        if keyvalues.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keyvalues))
        }
    }


    /// Flush the staging area and and move to work on a certain commit from history.
    pub fn checkout(&mut self, context_hash: &EntryHash) -> Result<(), MerkleError> {
        let commit = self.get_commit(&context_hash)?;
        self.current_stage_tree = Some(self.get_tree(&commit.root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        self.last_commit = Some(commit);
        self.staged = HashMap::new();
        self.map_stats.staged_area_elems = 0;
        Ok(())
    }

    /// Take the current changes in the staging area, create a commit and persist all changes
    /// to database under the new commit. Return last commit if there are no changes, that is
    /// empty commits are not allowed.
    pub fn commit(&mut self,
                  time: u64,
                  author: String,
                  message: String,
    ) -> Result<EntryHash, MerkleError> {
        let staged_root = self.get_staged_root()?;
        let staged_root_hash = self.hash_tree(&staged_root);
        let parent_commit_hash = self.last_commit.as_ref()
            .map_or(None, |c| Some(self.hash_commit(&c)));

        let new_commit = Commit {
            root_hash: staged_root_hash,
            parent_commit_hash,
            time,
            author,
            message,
        };
        let entry = Entry::Commit(new_commit.clone());

        self.put_to_staging_area(&self.hash_commit(&new_commit), entry.clone());
        self.persist_staged_entry_to_db(&entry)?;
        self.gc_entries_recursively(&entry);


        self.staged = HashMap::new();
        self.map_stats.staged_area_elems = 0;
        self.last_commit = Some(new_commit.clone());
        self.commits.insert(self.hash_commit(&new_commit));
        Ok(self.hash_commit(&new_commit))
    }

    /// Set key/val to the staging area.
    pub fn set(&mut self, key: &ContextKey, value: &ContextValue) -> Result<(), MerkleError> {
        let root = self.get_staged_root()?;
        let new_root_hash = &self._set(&root, key, value)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        Ok(())
    }

    fn _set(&mut self, root: &Tree, key: &ContextKey, value: &ContextValue) -> Result<EntryHash, MerkleError> {
        let blob_hash = self.hash_blob(&value);
        self.put_to_staging_area(&blob_hash, Entry::Blob(value.clone()));
        let new_node = Node { entry_hash: blob_hash, node_kind: NodeKind::Leaf };
        let instant = Instant::now();
        let rv = self.compute_new_root_with_change(root, &key, Some(new_node));
        let elapsed = instant.elapsed().as_nanos() as f64;
        if self.set_exec_times >= self.set_exec_times_to_discard.into() {
            self.cumul_set_exec_time += elapsed;
        }
        self.set_exec_times += 1;
        rv
    }

    /// Delete an item from the staging area.
    pub fn delete(&mut self, key: &ContextKey) -> Result<(), MerkleError> {
        let root = self.get_staged_root()?;
        let new_root_hash = &self._delete(&root, key)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        Ok(())
    }

    fn _delete(&mut self, root: &Tree, key: &ContextKey) -> Result<EntryHash, MerkleError> {
        if key.is_empty() { return Ok(self.hash_tree(root)); }

        self.compute_new_root_with_change(root, &key, None)
    }

    /// Copy subtree under a new path.
    /// TODO Consider copying values!
    pub fn copy(&mut self, from_key: &ContextKey, to_key: &ContextKey) -> Result<(), MerkleError> {
        let root = self.get_staged_root()?;
        let new_root_hash = &self._copy(&root, from_key, to_key)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.map_stats.current_tree_elems = self.current_stage_tree.as_ref().unwrap().len() as u64;
        Ok(())
    }

    fn _copy(&mut self, root: &Tree, from_key: &ContextKey, to_key: &ContextKey) -> Result<EntryHash, MerkleError> {
        let source_tree = self.find_tree(root, &from_key)?;
        let source_tree_hash = self.hash_tree(&source_tree);
        Ok(self.compute_new_root_with_change(
            &root, &to_key, Some(self.get_non_leaf(source_tree_hash)))?)
    }

    /// Get a new tree with `new_entry_hash` put under given `key`.
    ///
    /// # Arguments
    ///
    /// * `root` - Tree to modify
    /// * `key` - path under which the changes takes place
    /// * `new_entry_hash` - None for deletion, Some for inserting a hash under the key.
    fn compute_new_root_with_change(&mut self,
                                    root: &Tree,
                                    key: &[String],
                                    new_node: Option<Node>,
    ) -> Result<EntryHash, MerkleError> {
        if key.is_empty() {
            return Ok(new_node.unwrap_or_else(
                || self.get_non_leaf(self.hash_tree(root))).entry_hash);
        }

        let last = key.last().unwrap();
        let path = &key[..key.len() - 1];
        let mut tree = self.find_tree(root, path)?;

        match new_node {
            None => tree.remove(last),
            Some(new_node) => {
                tree.insert(last.clone(), new_node)
            }
        };

        if tree.is_empty() {
            self.compute_new_root_with_change(root, path, None)
        } else {
            let new_tree_hash = self.hash_tree(&tree);
            self.put_to_staging_area(&new_tree_hash, Entry::Tree(tree));
            self.compute_new_root_with_change(
                root, path, Some(self.get_non_leaf(new_tree_hash)))
        }
    }

    /// Find tree by path. Return an empty tree if no tree under this path exists or if a blob
    /// (= value) is encountered along the way.
    ///
    /// # Arguments
    ///
    /// * `root` - reference to a tree in which we search
    /// * `key` - sought path
    fn find_tree(&self, root: &Tree, key: &[String]) -> Result<Tree, MerkleError> {
        if key.is_empty() { return Ok(root.clone()); }

        let child_node = match root.get(key.first().unwrap()) {
            Some(hash) => hash,
            None => return Ok(Tree::new()),
        };

        match self.get_entry(&child_node.entry_hash)? {
            Entry::Tree(tree) => {
                self.find_tree(&tree, &key[1..])
            }
            Entry::Blob(_) => Ok(Tree::new()),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            })
        }
    }

    /// Get latest staged tree. If it's empty, init genesis  and return genesis root.
    fn get_staged_root(&mut self) -> Result<Tree, MerkleError> {
        match &self.current_stage_tree {
            None => {
                let tree = Tree::new();
                self.put_to_staging_area(&self.hash_tree(&tree), Entry::Tree(tree.clone()));
                self.map_stats.current_tree_elems = tree.len() as u64;
                Ok(tree)
            }
            Some(tree) => {
                self.map_stats.current_tree_elems = tree.len() as u64;
                Ok(tree.clone())
            }
        }
    }

    fn put_to_staging_area(&mut self, key: &EntryHash, value: Entry) {
        self.staged.insert(*key, value);
        self.map_stats.staged_area_elems = self.staged.len() as u64;
    }

    /// Persists an entry and its descendants from staged area to database on disk.
    fn persist_staged_entry_to_db(&mut self, entry: &Entry) -> Result<(), MerkleError> {
        let mut batch = Batch::default(); // batch containing DB key values to persist

        // build list of entries to be persisted
        self.get_entries_recursively(entry, &mut batch)?;

        // atomically write all entries in one batch to DB
        //self.db.write().unwrap().write_batch(batch)?;

        self.gc_entries_recursively(entry);

        Ok(())
    }

    fn gc_entries_recursively(&mut self, entry: &Entry) {
        let k = &self.hash_entry(entry);
        match entry {
            Entry::Blob(b) => {}
            Entry::Tree(tree) => {
                tree.iter().for_each(|(key, child_node)| {
                    self.gc.update(child_node.entry_hash);
                    match self.get_entry(&child_node.entry_hash) {
                        Err(_) => {}
                        Ok(entry) => self.gc_entries_recursively(&entry),
                    };
                });
            }
            Entry::Commit(commit) => {
                self.gc.update(commit.root_hash);
                match self.get_entry(&commit.root_hash) {
                    Err(_) => {}
                    Ok(entry) => {
                        self.gc_entries_recursively(&entry)
                    }
                }
            }
        }
    }

    fn delete_entries_recursively(&mut self, entry: &Entry) {
        let k = &self.hash_entry(entry);
        match entry {
            Entry::Blob(_) => {}
            Entry::Tree(tree) => {

                println!("Tree: {}", tree.len());
                tree.iter().for_each(|(key, child_node)| {
                    self.gc.delete(&self.hash_entry(&entry));
                    match self.get_entry(&child_node.entry_hash) {
                        Err(_) => {}
                        Ok(entry) => {
                            self.delete_entries_recursively(&entry)
                        },
                    };
                });
            }
            Entry::Commit(commit) => {
                match self.get_entry(&commit.root_hash) {
                    Err(_) => {}
                    Ok(entry) => {
                        self.delete_entries_recursively(&entry)
                    }
                }
            }
        }
    }


    /// Builds vector of entries to be persisted to DB, recursively
    fn get_entries_recursively(&self, entry: &Entry, batch: &mut Batch) -> Result<(), MerkleError> {
        // add entry to batch
        //self.db.apply_batch()

        let k = &self.hash_entry(entry);
        let v = bincode::serialize(entry)?;

        self.db.insert(k.clone(), v);
        match entry {
            Entry::Blob(_) => {
                //self.gc.update(k.clone(),None);
                Ok(())
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter().map(|(_, child_node)| {
                    //self.gc.update(k.clone(),Some(child_node.entry_hash));
                    match self.staged.get(&child_node.entry_hash) {
                        None => Ok(()),
                        Some(entry) => self.get_entries_recursively(entry, batch),
                    }
                }).find_map(|res| {
                    match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    }
                }).unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => {
                match self.get_entry(&commit.root_hash) {
                    Err(err) => Err(err),
                    Ok(entry) => self.get_entries_recursively(&entry, batch),
                }
            }
        }
    }

    fn hash_entry(&self, entry: &Entry) -> EntryHash {
        match entry {
            Entry::Commit(commit) => self.hash_commit(&commit),
            Entry::Tree(tree) => self.hash_tree(&tree),
            Entry::Blob(blob) => self.hash_blob(blob),
        }
    }

    pub fn clear_previous_commits(&mut self) -> Result<(), MerkleError> {
        let mut commits = self.commits.clone();

        if let Some(last_commit) = &self.last_commit {
            commits.remove(&self.hash_commit(last_commit));
        }

        for entry_hash in commits.iter().rev() {
            let commit = self.get_commit(entry_hash)?;
            let root_tree = self.get_tree(&commit.root_hash)?;
            println!("Root Tree: {}",root_tree.len());
            self.delete_entries_recursively(&Entry::Tree(root_tree))
        }
        self.gc.clean();
        Ok(())
    }

    fn hash_commit(&self, commit: &Commit) -> EntryHash {
        let mut hasher = State::new(HASH_LEN, None).unwrap();
        hasher.update(&(HASH_LEN as u64).to_be_bytes()).expect("hasher");
        hasher.update(&commit.root_hash).expect("hasher");

        if commit.parent_commit_hash.is_none() {
            hasher.update(&(0 as u64).to_be_bytes()).expect("hasher");
        } else {
            hasher.update(&(1 as u64).to_be_bytes()).expect("hasher"); // # of parents; we support only 1
            hasher.update(&(commit.parent_commit_hash.unwrap().len() as u64).to_be_bytes()).expect("hasher");
            hasher.update(&commit.parent_commit_hash.unwrap()).expect("hasher");
        }
        hasher.update(&(commit.time as u64).to_be_bytes()).expect("hasher");
        hasher.update(&(commit.author.len() as u64).to_be_bytes()).expect("hasher");
        hasher.update(&commit.author.clone().into_bytes()).expect("hasher");
        hasher.update(&(commit.message.len() as u64).to_be_bytes()).expect("hasher");
        hasher.update(&commit.message.clone().into_bytes()).expect("hasher");

        hasher.finalize().unwrap().as_ref().try_into().expect("EntryHash conversion error")
    }

    fn hash_tree(&self, tree: &Tree) -> EntryHash {
        let mut hasher = State::new(HASH_LEN, None).unwrap();

        hasher.update(&(tree.len() as u64).to_be_bytes()).expect("hasher");
        tree.iter().for_each(|(k, v)| {
            hasher.update(&self.encode_irmin_node_kind(&v.node_kind)).expect("hasher");
            hasher.update(&[k.len() as u8]).expect("hasher");
            hasher.update(&k.clone().into_bytes()).expect("hasher");
            hasher.update(&(HASH_LEN as u64).to_be_bytes()).expect("hasher");
            hasher.update(&v.entry_hash).expect("hasher");
        });

        hasher.finalize().unwrap().as_ref().try_into().expect("EntryHash conversion error")
    }

    fn hash_blob(&self, blob: &ContextValue) -> EntryHash {
        let mut hasher = State::new(HASH_LEN, None).unwrap();
        hasher.update(&(blob.len() as u64).to_be_bytes()).expect("Failed to update hasher state");
        hasher.update(blob).expect("Failed to update hasher state");

        hasher.finalize().unwrap().as_ref().try_into().expect("EntryHash conversion error")
    }

    fn encode_irmin_node_kind(&self, kind: &NodeKind) -> Vec<u8> {
        match kind {
            NodeKind::NonLeaf => vec![0, 0, 0, 0, 0, 0, 0, 0],
            NodeKind::Leaf => vec![255, 0, 0, 0, 0, 0, 0, 0],
        }
    }


    fn get_tree(&self, hash: &EntryHash) -> Result<Tree, MerkleError> {
        match self.get_entry(hash)? {
            Entry::Tree(tree) => Ok(tree),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "blob".to_string(),
            }),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    fn get_commit(&self, hash: &EntryHash) -> Result<Commit, MerkleError> {
        match self.get_entry(hash)? {
            Entry::Commit(commit) => Ok(commit),
            Entry::Tree(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "tree".to_string(),
            }),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "blob".to_string(),
            }),
        }
    }

    fn get_entry(&self, hash: &EntryHash) -> Result<Entry, MerkleError> {
        match self.staged.get(hash) {
            None => {
                let entry_bytes = self.db.get(hash);
                match entry_bytes {
                    None => Err(MerkleError::EntryNotFound { hash: HashType::ContextHash.bytes_to_string(hash) }),
                    Some(entry_bytes) => {
                        Ok(bincode::deserialize(entry_bytes.value())?)
                    }
                }
            }
            Some(entry) => Ok(entry.clone()),
        }
    }

    fn get_non_leaf(&self, hash: EntryHash) -> Node {
        Node { node_kind: NodeKind::NonLeaf, entry_hash: hash }
    }

    fn key_to_string(&self, key: &ContextKey) -> String {
        key.join("/")
    }

    fn string_to_key(&self, string: &str) -> ContextKey {
        string.split('/').map(str::to_string).collect()
    }

    pub fn get_last_commit_hash(&self) -> Option<EntryHash> {
        match &self.last_commit {
            Some(c) => Some(self.hash_commit(&c)),
            None => None
        }
    }

    pub fn get_merkle_stats(&self) -> Result<MerkleStorageStats, MerkleError> {
        let mut avg_set_exec_time_ns: f64 = 0.0;
        if self.set_exec_times > self.set_exec_times_to_discard {
            avg_set_exec_time_ns = self.cumul_set_exec_time / ((self.set_exec_times - self.set_exec_times_to_discard) as f64);
        }
        let perf = MerklePerfStats { avg_set_exec_time_ns: avg_set_exec_time_ns };
        let db_stats = DBStats {
            db_size: 0,
            keys: self.db.len(),
        };
        //let db_reader = self.db.read().unwrap();
        //let db_stats = db_reader.get_mem_use_stats().unwrap_or(DBStats { db_size: 0, keys: 0 });
        Ok(MerkleStorageStats { db_stats, map_stats: self.map_stats, perf_stats: perf })
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use super::*;
    use serial_test::serial;
    use crate::database::{DB};

    /*
    * Tests need to run sequentially, otherwise they will try to open RocksDB at the same time.
    */
    fn get_storage() -> MerkleStorage { MerkleStorage::new(Arc::new(DashMap::new())) }

    fn clean_db() {}

    #[test]
    #[serial]
    fn test_tree_hash() {
        let mut storage = get_storage();
        storage.set(&vec!["a".to_string(), "foo".to_string()], &vec![97, 98, 99]); // abc
        storage.set(&vec!["b".to_string(), "boo".to_string()], &vec![97, 98]);
        storage.set(&vec!["a".to_string(), "aaa".to_string()], &vec![97, 98, 99, 100]);
        storage.set(&vec!["x".to_string()], &vec![97]);
        storage.set(&vec!["one".to_string(), "two".to_string(), "three".to_string()], &vec![97]);
        let tree = storage.current_stage_tree.clone().unwrap().clone();

        let hash = storage.hash_tree(&tree);

        assert_eq!([0xDB, 0xAE, 0xD7, 0xB6], hash[0..4]);
    }

    #[test]
    #[serial]
    fn test_commit_hash() {
        let mut storage = get_storage();
        storage.set(&vec!["a".to_string()], &vec![97, 98, 99]);

        let commit = storage.commit(
            0, "Tezos".to_string(), "Genesis".to_string());

        assert_eq!([0xCF, 0x95, 0x18, 0x33], commit.unwrap()[0..4]);

        storage.set(&vec!["data".to_string(), "x".to_string()], &vec![97]);
        let commit = storage.commit(
            0, "Tezos".to_string(), "".to_string());

        assert_eq!([0xCA, 0x7B, 0xC7, 0x02], commit.unwrap()[0..4]);
        // full irmin hash: ca7bc7022ffbd35acc97f7defb00c486bb7f4d19a2d62790d5949775eb74f3c8
    }

    #[test]
    #[serial]
    fn test_multiple_commit_hash() {
        let mut storage = get_storage();
        let _commit = storage.commit(
            0, "Tezos".to_string(), "Genesis".to_string());

        storage.set(&vec!["data".to_string(), "a".to_string(), "x".to_string()], &vec![97]);
        storage.copy(&vec!["data".to_string(), "a".to_string()], &vec!["data".to_string(), "b".to_string()]);
        storage.delete(&vec!["data".to_string(), "b".to_string(), "x".to_string()]);
        let commit = storage.commit(
            0, "Tezos".to_string(), "".to_string());
        assert_eq!([0x9B, 0xB0, 0x0D, 0x6E], commit.unwrap()[0..4]);
    }

    #[test]
    #[serial]
    fn get_test() {
        clean_db();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        let key_eab: &ContextKey = &vec!["e".to_string(), "a".to_string(), "b".to_string()];
        let key_az: &ContextKey = &vec!["a".to_string(), "z".to_string()];
        let key_d: &ContextKey = &vec!["d".to_string()];

        let mut storage = get_storage();

        {
            storage.set(key_abc, &vec![1u8, 2u8]);
            storage.set(key_abx, &vec![3u8]);
            assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8, 2u8]);
            assert_eq!(storage.get(&key_abx).unwrap(), vec![3u8]);
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

            storage.set(key_az, &vec![4u8]);
            storage.set(key_abx, &vec![5u8]);
            storage.set(key_d, &vec![6u8]);
            storage.set(key_eab, &vec![7u8]);
            assert_eq!(storage.get(key_abx).unwrap(), vec![5u8]);
            commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        storage.clear_previous_commits();
        println!("{:#?}", storage.get_merkle_stats());
        assert_eq!(storage.get_history(&commit1, key_abc).unwrap(), vec![1u8, 2u8]);
        assert_eq!(storage.get_history(&commit1, key_abx).unwrap(), vec![3u8]);
        assert_eq!(storage.get_history(&commit2, key_abx).unwrap(), vec![5u8]);
        assert_eq!(storage.get_history(&commit2, key_az).unwrap(), vec![4u8]);
        assert_eq!(storage.get_history(&commit2, key_d).unwrap(), vec![6u8]);
        assert_eq!(storage.get_history(&commit2, key_eab).unwrap(), vec![7u8]);
    }

    #[test]
    #[serial]
    fn garbage_test() {
        clean_db();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        let key_eab: &ContextKey = &vec!["e".to_string(), "a".to_string(), "b".to_string()];
        let key_az: &ContextKey = &vec!["a".to_string(), "z".to_string()];
        let key_d: &ContextKey = &vec!["d".to_string()];
        let key_h: &ContextKey = &vec!["h".to_string()];
        let key_z: &ContextKey = &vec!["z".to_string()];

        let mut storage = get_storage();

        {
            storage.set(key_h, &vec![1u8]);
            storage.set(key_z, &vec![8u8]);
            storage.set(key_abc, &vec![1u8, 2u8]);
            storage.set(key_abx, &vec![3u8]);
            assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8, 2u8]);
            assert_eq!(storage.get(&key_abx).unwrap(), vec![3u8]);
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
            storage.set(key_az, &vec![4u8]);
            storage.set(key_abx, &vec![5u8]);
            storage.set(key_d, &vec![6u8]);
            storage.set(key_eab, &vec![7u8]);
            assert_eq!(storage.get(key_abx).unwrap(), vec![5u8]);
            commit2 = storage.commit(0, "".to_string(), "2".to_string()).unwrap();
        }
        println!("Before GC: {:#?}", storage.get_merkle_stats());
        println!("Before GC: {}", storage.gc);
        storage.clear_previous_commits();
        println!("After GC: {:#?}", storage.get(key_abc));
        println!("After GC: {:#?}", storage.get_merkle_stats());
        println!("After GC: {}", storage.gc);
        /*

        assert_eq!(storage.get_history(&commit1, key_abc).unwrap(), vec![1u8, 2u8]);
        assert_eq!(storage.get_history(&commit1, key_abx).unwrap(), vec![3u8]);
        assert_eq!(storage.get_history(&commit2, key_abx).unwrap(), vec![5u8]);
        assert_eq!(storage.get_history(&commit2, key_az).unwrap(), vec![4u8]);
        assert_eq!(storage.get_history(&commit2, key_d).unwrap(), vec![6u8]);
        assert_eq!(storage.get_history(&commit2, key_eab).unwrap(), vec![7u8]);

         */
    }

    #[test]
    #[serial]
    fn test_copy() {
        clean_db();
        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, &vec![1 as u8]);
        storage.copy(&vec!["a".to_string()], &vec!["z".to_string()]);

        assert_eq!(
            vec![1 as u8],
            storage.get(&vec!["z".to_string(), "b".to_string(), "c".to_string()]).unwrap());
        // TODO test copy over commits
    }

    #[test]
    #[serial]
    fn test_delete() {
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, &vec![2 as u8]);
        storage.set(key_abx, &vec![3 as u8]);
        storage.delete(key_abx);
        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

        assert!(storage.get_history(&commit1, &key_abx).is_err());
    }

    #[test]
    #[serial]
    fn test_deleted_entry_available() {
        clean_db();
        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, &vec![2 as u8]);
        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        storage.delete(key_abc);
        let _commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

        assert_eq!(vec![2 as u8], storage.get_history(&commit1, &key_abc).unwrap());
    }

    #[test]
    #[serial]
    fn test_delete_in_separate_commit() {
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, &vec![2 as u8]).unwrap();
        storage.set(key_abx, &vec![3 as u8]).unwrap();
        storage.commit(0, "".to_string(), "".to_string()).unwrap();

        storage.delete(key_abx);
        let commit2 = storage.commit(
            0, "".to_string(), "".to_string()).unwrap();

        assert!(storage.get_history(&commit2, &key_abx).is_err());
    }

    #[test]
    #[serial]
    fn test_checkout() {
        clean_db();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        let mut storage = get_storage();
        {
            storage.set(key_abc, &vec![1u8]).unwrap();
            storage.set(key_abx, &vec![2u8]).unwrap();
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

            storage.set(key_abc, &vec![3u8]).unwrap();
            storage.set(key_abx, &vec![4u8]).unwrap();
            commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        storage.checkout(&commit1);
        assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![2u8]);
        // this set be wiped by checkout
        storage.set(key_abc, &vec![8u8]).unwrap();

        storage.checkout(&commit2);
        assert_eq!(storage.get(&key_abc).unwrap(), vec![3u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![4u8]);
    }


    #[test]
    #[serial]
    fn test_persistence_over_reopens() {
        { clean_db(); }

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let commit1;
        let mut storage = get_storage();
        {
            let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
            storage.set(key_abc, &vec![2 as u8]).unwrap();
            storage.set(key_abx, &vec![3 as u8]).unwrap();
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        assert_eq!(vec![2 as u8], storage.get_history(&commit1, &key_abc).unwrap());
    }

    #[test]
    #[serial]
    fn test_get_errors() {
        { clean_db(); }

        let mut storage = get_storage();

        let res = storage.get(&vec![]);
        assert!(if let MerkleError::KeyEmpty = res.err().unwrap() { true } else { false });

        let res = storage.get(&vec!["a".to_string()]);
        assert!(if let MerkleError::ValueNotFound { .. } = res.err().unwrap() { true } else { false });
    }

    // Test getting entire tree in string format for JSON RPC
    #[test]
    fn test_get_context_tree_by_prefix() {
        { clean_db(); }

        let all_json = "[[[\"adata\",\"b\",\"x\",\"y\"],[12,15]],[[\"data\",\"a\",\"x\",\"y\"],[5,6]],[[\"data\",\"b\",\"x\",\"y\"],[7,8]],[[\"data\",\"c\"],[2,5]]]";
        let data_json = "[[[\"data\",\"a\",\"x\",\"y\"],[5,6]],[[\"data\",\"b\",\"x\",\"y\"],[7,8]],[[\"data\",\"c\"],[2,5]]]";

        let mut storage = get_storage();
        let _commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());

        storage.set(&vec!["data".to_string(), "a".to_string(), "x".to_string()], &vec![3, 4]);
        storage.set(&vec!["data".to_string(), "a".to_string()], &vec![20, 30]);
        storage.set(&vec!["data".to_string(), "a".to_string(), "x".to_string(), "y".to_string()], &vec![5, 6]);
        storage.set(&vec!["data".to_string(), "b".to_string(), "x".to_string(), "y".to_string()], &vec![7, 8]);
        storage.set(&vec!["data".to_string(), "c".to_string()], &vec![2, 5]);
        storage.set(&vec!["adata".to_string(), "b".to_string(), "x".to_string(), "y".to_string()], &vec![12, 15]);

        let commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string()).unwrap();

        let rv_all = storage.get_key_values_by_prefix(&EntryHash::decode(&commit).unwrap(), &vec![]).unwrap();
        let rv_data = storage.get_key_values_by_prefix(&EntryHash::decode(&commit).unwrap(), &vec!["data".to_string()]).unwrap();
        assert_eq!(all_json, serde_json::to_string(&rv_all.unwrap()).unwrap());
        assert_eq!(data_json, serde_json::to_string(&rv_data.unwrap()).unwrap());
    }
}