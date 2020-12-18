use crate::schema::KeyValueSchema;
use crate::codec::{SchemaError, Encoder, Decoder};
use failure::Fail;
use std::marker::PhantomData;
use crate::db_iterator;
use std::collections::{HashMap, BTreeMap};
use crate::db_iterator::{DBIterator, DBIterationHandler};
use crate::ivec::IVec;
use patricia_tree::PatriciaMap;
use crate::merkle_storage::EntryHash;

#[derive(Debug, Default, Clone)]
pub struct Batch {
    pub(crate) writes: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl Batch {
    /// Set a key to a new value
    pub fn insert<K, V>(&mut self, key: K, value: V)
        where
            K: Into<Vec<u8>>,
            V: Into<Vec<u8>>,
    {
        self.writes.insert(key.into(), Some(value.into()));
    }

    /// Remove a key
    pub fn remove<K>(&mut self, key: K)
        where
            K: Into<Vec<u8>>,
    {
        self.writes.insert(key.into(), None);
    }
}

//Mark: Hack
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    pub writes: HashMap<EntryHash, Vec<u8>>,
}

impl WriteBatch {
    /// Set a key to a new value
    pub fn insert<V>(&mut self, key: EntryHash, value: V)
        where
            V: Into<Vec<u8>>,
    {
        self.writes.insert(key.into(), value.into());
    }

    /// Remove a key
    pub fn remove(&mut self, key: &EntryHash)
    {
        self.writes.remove(key);
    }
}

impl From<SchemaError> for DBError {
    fn from(error: SchemaError) -> Self {
        DBError::SchemaError { error }
    }
}

#[derive(Debug, Fail)]
pub enum DBError {

    #[fail(display = "Not found error")]
    NotFoundErr,

    #[fail(display = "Schema error: {}", error)]
    SchemaError {
        error: SchemaError
    },
}
impl slog::Value for DBError {
    fn serialize(&self, _record: &slog::Record, key: slog::Key, serializer: &mut dyn slog::Serializer) -> slog::Result {
        serializer.emit_arguments(key, &format_args!("{}", self))
    }
}

pub struct DBStats {
}


/// Custom trait extending RocksDB to better handle and enforce database schema
pub trait KeyValueStoreWithSchema<S: KeyValueSchema> {
    /// Insert new key value pair into the database. If key already exists, method will fail
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn put(&mut self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Delete existing value associated with given key from the database.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    fn delete(&mut self, key: &S::Key) -> Result<(), DBError>;

    /// Insert key value pair into the database, overriding existing value if exists.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn merge(&mut self, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Read value associated with given key, if exists.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError>;

    /// Read all entries in database.
    ///
    /// # Arguments
    /// * `mode` - Reading mode, specified by RocksDB, From start to end, from end to start, or from
    /// arbitrary position to end.
    fn iterator(&self, mode: IteratorMode<S>) -> Result<IteratorWithSchema<S>, DBError>;

    /// Starting from given key, read all entries to the end.
    ///
    /// # Arguments
    /// * `key` - Key (specified by schema), from which to start reading entries
    fn prefix_iterator(&self, key: &S::Key) -> Result<IteratorWithSchema<S>, DBError>;

    /// Check, if database contains given key
    ///
    /// # Arguments
    /// * `key` - Key (specified by schema), to be checked for existence
    fn contains(&self, key: &S::Key) -> Result<bool, DBError>;

    /// Insert new key value pair into WriteBatch.
    ///
    /// # Arguments
    /// * `key` - Value of key specified by schema
    /// * `value` - Value to be inserted associated with given key, specified by schema
    fn put_batch(&self, batch: &mut Batch, key: &S::Key, value: &S::Value) -> Result<(), DBError>;

    /// Write batch into DB atomically
    ///
    /// # Arguments
    /// * `batch` - WriteBatch containing all batched writes to be written to DB
    fn write_batch(&mut self, batch: Batch) -> Result<(), DBError>;

    /// Get memory usage statistics from DB
    fn get_mem_use_stats(&self) -> Result<DBStats, DBError>;
}

pub struct IteratorWithSchema<'a, S: KeyValueSchema>(DBIterator<'a>, PhantomData<S>);

impl<'a, S: KeyValueSchema> Iterator for IteratorWithSchema<'a, S> {
    type Item = (Result<S::Key, SchemaError>, Result<S::Value, SchemaError>);

    fn next(&mut self) -> Option<Self::Item> {
        let (k,v) = match self.0.next() {
            None => {
                return None;
            }
            Some(d) => {
                d
            }
        };
        Some((S::Key::decode(&k), S::Value::decode(&v)))
    }
}

pub struct DB {
    pub(crate) inner : PatriciaMap<Vec<u8>>
}

impl DB {

    pub fn new() -> Self {
        DB {
            inner: PatriciaMap::new()
        }
    }

    pub(crate) fn apply_batch(&mut self, batch: Batch) {
        batch.writes.iter().for_each(|(k,v)| {
            match v {
                None => {}
                Some(v) => {
                    self.inner.insert(k.clone(),v.clone());
                }
            };
        });
    }
}

/// Database iterator direction
pub enum Direction {
    Forward,
    Reverse,
}

/// Database iterator with schema mode, from start to end, from end to start or from specific key to end/start
pub enum IteratorMode<'a, S: KeyValueSchema> {
    Start,
    End,
    From(&'a S::Key, Direction),
}

impl<S: KeyValueSchema> KeyValueStoreWithSchema<S> for DB {
    fn put(&mut self, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        self.inner.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &S::Key) -> Result<(), DBError> {
        let key = key.encode()?;
        self.inner.remove(key);
        Ok(())
    }

    fn merge(&mut self, key: &S::Key, value: &<S as KeyValueSchema>::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        self.inner.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &S::Key) -> Result<Option<S::Value>, DBError> {
        let key = key.encode()?;

        match self.inner.get(&IVec::from(key)) {
            Some(v) => {
                Ok(Some(S::Value::decode(v)?))
            }
            None => {
                Err(DBError::NotFoundErr)
            }
        }
    }

    fn iterator(&self, mode: IteratorMode<S>) -> Result<IteratorWithSchema<S>, DBError> {
        let iter = match mode {
            IteratorMode::Start => {
                self.iter(db_iterator::IteratorMode::Start)
            }
            IteratorMode::End => {
                self.iter(db_iterator::IteratorMode::End)
            }
            IteratorMode::From(key, direction) => {
                let key = key.encode()?;
                match direction {
                    Direction::Forward => {
                        self.iter(db_iterator::IteratorMode::From(key.into(), db_iterator::Direction::Forward))
                    }
                    Direction::Reverse => {
                        self.iter(db_iterator::IteratorMode::From(key.into(), db_iterator::Direction::Reverse))
                    }
                }
            }
        };
        Ok(IteratorWithSchema(iter, PhantomData))
    }

    fn prefix_iterator(&self, key: &S::Key) -> Result<IteratorWithSchema<S>, DBError> {
        let key = key.encode()?;
        let iter = self.scan_prefix(&key);
        Ok(IteratorWithSchema(iter, PhantomData))
    }

    fn contains(&self, key: &S::Key) -> Result<bool, DBError> {
        let key = key.encode()?;
        Ok(self.inner.contains_key(&key))
    }

    fn put_batch(&self, batch: &mut Batch, key: &S::Key, value: &S::Value) -> Result<(), DBError> {
        let key = key.encode()?;
        let value = value.encode()?;
        batch.insert(key, value);
        Ok(())
    }

    fn write_batch(&mut self, batch: Batch) -> Result<(), DBError> {
        self.apply_batch(batch);
        Ok(())
    }

    fn get_mem_use_stats(&self) -> Result<DBStats, DBError> {
        Ok(DBStats {})
    }
}


