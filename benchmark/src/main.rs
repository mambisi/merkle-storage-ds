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
use std::collections::BTreeMap;
use clap::Arg;
use sysinfo::{SystemExt, Process, ProcessExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = clap::App::new("Merkle Storage Benchmark")
        .author("mambisi.zempare@simplestaking.com")
        .arg(Arg::with_name("node")
            .short("n")
            .long("node")
            .value_name("NODE")
            .takes_value(true)
            .default_value("http://127.0.0.1:18732")
            .help("Node base url")
        )
        .arg(Arg::with_name("limit")
            .short("l")
            .long("limit")
            .value_name("LIMIT")
            .takes_value(true)
            .default_value("25000")
            .help("Specifies the block height limit")
        )
        .arg(Arg::with_name("cycle")
            .short("c")
            .long("cycle")
            .value_name("CYCLE")
            .takes_value(true)
            .default_value("4096")
            .help("Cycle length, logs the memory usage at every cycle")
        )
        .get_matches();

    let node = matches.value_of("node").unwrap();
    let blocks_limit = matches.value_of("limit").unwrap().parse::<u64>().unwrap_or(25000);
    let cycle = matches.value_of("cycle").unwrap().parse::<u64>().unwrap_or(4096);
    let process_id = std::process::id();

    println!("node {}, limit {}, process id: {}", node, blocks_limit, process_id);


    run_benchmark(process_id, node, blocks_limit, cycle).await
}


async fn run_benchmark(process_id: u32, node: &str, blocks_limit: u64, cycle: u64) -> Result<(), Box<dyn std::error::Error>> {
    let blocks_url = format!("{}/dev/chains/main/blocks?limit={}&from_block_id={}", node, blocks_limit + 10, blocks_limit);
    let db = Arc::new(RwLock::new(DB::new()));
    let mut storage = MerkleStorage::new(db.clone());

    let mut blocks = reqwest::get(&blocks_url)
        .await?
        .json::<Vec<Value>>()
        .await?;
    blocks.reverse();

    for block in &blocks {
        let block = block.as_object().unwrap();
        let block_hash = block.get("hash").unwrap().as_str();
        let block_header = block.get("header").unwrap().as_object().unwrap();
        let block_level = block_header.get("level").unwrap().as_u64().unwrap();
        let block_hash = block_hash.unwrap();
        let actions_url = format!("{}/dev/chains/main/actions/blocks/{}", node, block_hash);
        let mut messages = reqwest::get(&actions_url)
            .await?
            .json::<Vec<ContextActionJson>>()
            .await?;

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


        let system = sysinfo::System::new();
        match system.get_process(process_id as i32) {
            None => {}
            Some(process) => {
                if block_level % cycle == 0 {
                    println!("Blocks Applied: {}", block_level);
                    println!("Memory Stats: {}", process.memory());
                    println!("Virtual Memory Stats: {}", process.virtual_memory())
                }
            }
        };
    }
    Ok(())
}
