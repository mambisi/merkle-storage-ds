#![feature(async_closure)]
extern crate merkle;

mod channel;

use merkle::prelude::*;
use std::sync::{Arc, RwLock, Mutex};
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
use tokio::process::Command;
use std::process::Output;
use tokio::io::Error;
use tokio::macros::support::Future;
use std::io;

/*
use jemalloc_ctl::{stats, epoch};

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
*/
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
        .arg(Arg::with_name("gc")
            .short("gc")
            .long("gc")
            .help("Garbage Collection")
        )
        .get_matches();

    let node = matches.value_of("node").unwrap();
    let blocks_limit = matches.value_of("limit").unwrap().parse::<u64>().unwrap_or(25000);
    let cycle = matches.value_of("cycle").unwrap().parse::<u64>().unwrap_or(4096);
    let gc_enabled = matches.is_present("gc");
    let process_id = std::process::id();

    println!("node {}, limit {}, process id: {}, gc : {}", node, blocks_limit, process_id, if gc_enabled {"enabled"} else {"disabled"} );
    run_benchmark(gc_enabled, node, blocks_limit, cycle).await
}


async fn run_benchmark(gc_enabled: bool, node: &str, blocks_limit: u64, cycle: u64) -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(RwLock::new(DB::new()));
    let mut storage = MerkleStorage::new(db.clone());
    let mut current_cycle = 0;
    let e = epoch::mib().unwrap();
    let allocated = stats::allocated::mib().unwrap();
    let resident = stats::resident::mib().unwrap();

    for i in 0..blocks_limit {
        let blocks_url = format!("{}/dev/chains/main/blocks?limit={}&from_block_id={}", node, 1, i);
        let mut blocks = reqwest::get(&blocks_url)
            .await?
            .json::<Vec<Value>>()
            .await?;

        let block = blocks.first().unwrap().as_object().unwrap();
        let block_hash = block.get("hash").unwrap().as_str();
        let block_header = block.get("header").unwrap().as_object().unwrap();
        let block_level = block_header.get("level").unwrap().as_u64().unwrap();
        let block_hash = block_hash.unwrap();
        let actions_url = format!("{}/dev/chains/main/actions/blocks/{}", node, block_hash);

        drop(block);


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
                    assert_eq!(
                        &commit_hash,
                        new_context_hash,
                        "Invalid context_hash for block: {}, expected: {}, but was: {}",
                        HashType::BlockHash.hash_to_b58check(block_hash),
                        HashType::ContextHash.hash_to_b58check(new_context_hash),
                        HashType::ContextHash.hash_to_b58check(&hash),
                    );
                }

                ContextAction::Checkout { context_hash, .. } => {
                    let context_hash_arr: EntryHash = context_hash.as_slice().try_into().unwrap();
                    storage.checkout(&context_hash_arr);
                }
                _ => (),
            };
        }
        if block_level != 0 && block_level % cycle == 0 {
            current_cycle += 1;
            e.advance().unwrap();
            if gc_enabled {
                storage.gc();
                println!("DB(Patricia Tree) stats (GC)  at cycle: {}", current_cycle);
            }else {
                println!("DB(Patricia Tree) stats No GC  at cycle: {}", current_cycle);
            }
            match storage.get_merkle_stats() {
                Ok(stats) => {
                    //let mem_allocated = allocated.read().unwrap();
                    //let mem_resident = resident.read().unwrap();
                    print_stats(stats, 0, 0)
                }
                Err(_) => {}
            };
        }
    }
    Ok(())
}

fn print_stats(stats: MerkleStorageStats, mem_allocated: usize, mem_resident: usize) {
    // Read statistics using MIB key:
    //println!("      {:<35}{:.2} MiB", "RESIDENT MEM:", mem_resident as f64 / 1024.0 / 1024.0);
    //println!("      {:<35}{:.2} MiB", "ALLOCATED MEM:", mem_allocated as f64 / 1024.0 / 1024.0);
    //println!("      {:<35}{:.2} MiB", "DB SIZE:", stats.db_stats.db_size as f64 / 1024.0 / 1024.0);
    println!("      {:<35}{}", "KEYS:", stats.db_stats.keys);

    for (k, v) in stats.perf_stats.global.iter() {
        println!("      {:<35}{}", format!("{} AVG EXEC TIME:", k.to_uppercase()), v.avg_exec_time);
        println!("      {:<35}{}", format!("{} OP EXEC TIME MAX:", k.to_uppercase()), v.op_exec_time_max);
        println!("      {:<35}{}", format!("{} OP EXEC TIME MIN:", k.to_uppercase()), v.op_exec_time_min);
        println!("      {:<35}{}", format!("{} OP EXEC TIMES:", k.to_uppercase()), v.op_exec_times);
    }

}