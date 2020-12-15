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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut storage = MerkleStorage::new(Arc::new(RwLock::new(DB::new())));
    //env!("BASE_URL")
    let base_url = "http://46.101.190.161:18732";

    let blocks_url = format!("{}/dev/chains/main/blocks", base_url);
    let mut blocks = reqwest::get(&blocks_url)
        .await?
        .json::<Vec<Value>>()
        .await?;
    //blocks.reverse();
    for block in &blocks {
        let block = block.as_object().unwrap();
        let block_hash = block.get("hash").unwrap().as_str();
        let actions_url = format!("{}/dev/chains/main/actions/blocks/{}", base_url, block_hash.unwrap());
        let mut messages = reqwest::get(&actions_url)
            .await?
            .json::<Vec<ContextActionJson>>()
            .await?;

        messages.reverse();

        if messages.len() > 0 {
            println!("{:?}", messages.len())
        }

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
                    println!("context hash: {} commit_hash: {}", HashType::ContextHash.bytes_to_string(new_context_hash), HashType::ContextHash.bytes_to_string(&commit_hash));
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


    Ok(())
}
