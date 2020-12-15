extern crate merkle;

mod channel;

use merkle::prelude::*;
use std::sync::{Arc, RwLock};
use rayon::prelude::*;
use std::time::Instant;
use rand::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use timer;
use chrono::Duration;
use crate::channel::{ContextAction, ContextActionJson};
use std::io::{Cursor, Read};
use serde_json::{Value, Map};
use std::convert::TryInto;


fn main() {
    let json_file = include_bytes!("test_actions.json");
    let mut messages: Vec<ContextActionJson> = serde_json::from_slice(json_file).unwrap();

    let mut storage = MerkleStorage::new(Arc::new(RwLock::new(DB::new())));

    for msg in &messages {
        match &msg.action {
            ContextAction::Set { key, value, context_hash, ignored, .. } =>
                if !ignored {
                    storage.set(key, value);
                }
            ContextAction::Copy { to_key: key, from_key, context_hash, ignored, .. } =>
                if !ignored {
                    storage.copy(from_key, key);
                }
            ContextAction::Delete { key, context_hash, ignored, .. } =>
                if !ignored {
                    storage.delete(key);
                }
            ContextAction::RemoveRecursively { key, context_hash, ignored, .. } =>
                if !ignored {
                    storage.delete(key);
                }
            ContextAction::Commit {
                parent_context_hash, new_context_hash, block_hash: Some(block_hash),
                author, message, date, ..
            } => {
                let date = *date as u64;
                let hash = storage.commit(date, author.to_owned(), message.to_owned()).unwrap();
                let commit_hash = hash[..].to_vec();
                assert_eq!(&commit_hash, new_context_hash,
                           "Invalid context_hash for block: {}, expected: {}, but was: {}",
                           HashType::BlockHash.bytes_to_string(block_hash),
                           HashType::ContextHash.bytes_to_string(new_context_hash),
                           HashType::ContextHash.bytes_to_string(&commit_hash),
                );
            }

            ContextAction::Checkout { context_hash, .. } => {
                let context_hash_arr: EntryHash = context_hash.as_slice().try_into().unwrap();
                storage.checkout(&context_hash_arr);
            }
            _ => (),
        };
    }
}
