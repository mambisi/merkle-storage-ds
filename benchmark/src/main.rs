#![feature(async_closure)]
extern crate merkle;

mod channel;
mod util;

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
use tui::backend::TermionBackend;
use tui::Terminal;
use tui::layout::*;
use tui::widgets::*;
use tui::style::*;
use tui::backend::*;
use termion::raw::IntoRawMode;
use termion::screen::AlternateScreen;
use termion::input::Events;
use termion::input::MouseTerminal;
use termion::event::{Key};
use tui::text::Span;
use util::Event;
use tui::buffer::Cell;
use tokio::sync::RwLockReadGuard;

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

    let node = matches.value_of("node").unwrap().to_string();
    let blocks_limit = matches.value_of("limit").unwrap().parse::<u64>().unwrap_or(25000);
    let cycle = matches.value_of("cycle").unwrap().parse::<u64>().unwrap_or(4096);
    let process_id = std::process::id();

    println!("node {}, limit {}, process id: {}", node, blocks_limit, process_id);

    let mut ui = Arc::new(RwLock::new(BenchUI::default()));
    let ui2 = ui.clone();
    tokio::spawn(async move {
        let node = node;
        run_benchmark(process_id, ui2, &node, blocks_limit, cycle).await;
    });

    run(ui);

    Ok(())
}


async fn run_benchmark(process_id: u32, ui: Arc<RwLock<BenchUI>>, node: &str, blocks_limit: u64, cycle: u64) -> Result<(), Box<dyn std::error::Error>> {
    let blocks_url = format!("{}/dev/chains/main/blocks?limit={}&from_block_id={}", node, blocks_limit + 10, blocks_limit);
    let db = Arc::new(RwLock::new(DB::new()));
    let mut storage = MerkleStorage::new(db.clone());
    let mut current_cycle = 0;


    {
        let mut ui = ui.write().unwrap();
        ui.logs.push(String::from("Requesting Blocks..."));
    }
    let mut blocks = reqwest::get(&blocks_url)
        .await?
        .json::<Vec<Value>>()
        .await?;

    blocks.reverse();

    {
        let mut ui = ui.write().unwrap();
        ui.total_blocs = blocks.len() as f64;
    }


    for block in blocks {
        let block = block.as_object().unwrap();
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


        match storage.get_merkle_stats() {
            Ok(stats) => {
                let mut ui = ui.write().unwrap();
                ui.synced_blocks = block_level as f64;
                ui.stats = Some(stats)
            }
            Err(_) => {}
        };


        if block_level != 0 && block_level % cycle == 0 {
            current_cycle += 1;


            let pid = process_id.to_string();
            if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
                let output = Command::new("ps")
                    .arg("-p")
                    .arg(&pid)
                    .arg("-o")
                    .arg("pid,%mem,rss,vsize")
                    .output().await;
                match output {
                    Ok(output) => {
                        let mut ui = ui.write().unwrap();
                        ui.synced_blocks = block_level as f64;
                        ui.logs.push(format!("{}", String::from_utf8_lossy(&output.stdout)))
                    }
                    Err(_) => {
                        let mut ui = ui.write().unwrap();
                        ui.synced_blocks = block_level as f64;
                        ui.logs.push(String::from("Error executing PS"));
                    }
                }
            }


            println!("DB stats (Before GC)  at cycle: {}", current_cycle);
            match storage.get_merkle_stats() {
                Ok(stats) => {
                    let mut ui = ui.write().unwrap();
                    ui.synced_blocks = block_level as f64;
                    ui.logs.push(format!("{:#?}", stats));
                }
                Err(_) => {}
            };
            storage.gc();
            println!("DB stats (After GC)  at cycle: {}", current_cycle);
            match storage.get_merkle_stats() {
                Ok(stats) => {
                    let mut ui = ui.write().unwrap();
                    ui.synced_blocks = block_level as f64;
                    ui.logs.push(format!("{:#?}", stats));
                }
                Err(_) => {}
            };
        }
    }
    Ok(())
}

struct BenchUI {
    synced_blocks: f64,
    total_blocs: f64,
    stats: Option<MerkleStorageStats>,
    logs: Vec<String>,
}

impl Default for BenchUI {
    fn default() -> Self {
        BenchUI {
            synced_blocks: 0.0,
            total_blocs: 0.0,
            stats: None,
            logs: vec![],
        }
    }
}

fn run(app: Arc<RwLock<BenchUI>>) -> Result<(), Box<dyn std::error::Error>> {

    // Terminal initialization
    let stdout = io::stdout().into_raw_mode()?;
    let stdout = MouseTerminal::from(stdout);
    let stdout = AlternateScreen::from(stdout);
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let events = util::Events::new();
    loop {


        terminal.draw(|f| {


            let app = app.read().unwrap();

            let text: String = String::from("Test Strings");
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .margin(1)
                .constraints(
                    [
                        Constraint::Percentage(60),
                        Constraint::Percentage(40),
                    ]
                        .as_ref(),
                )
                .split(f.size());

            let left_chunk = Layout::default()
                .direction(Direction::Vertical)
                .margin(2)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Max(100),
                    ]
                        .as_ref(),
                )
                .split(chunks[0]);
            let right_chunk = Layout::default()
                .direction(Direction::Vertical)
                .margin(2)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Length(3),
                    ]
                        .as_ref(),
                )
                .split(chunks[1]);


            let gauge = Gauge::default()
                .block(Block::default().title("Sync Progress").borders(Borders::ALL))
                .gauge_style(Style::default().fg(Color::Yellow)).ratio(app.synced_blocks / if app.total_blocs == 0.0 { 1_f64 } else { app.total_blocs });
            f.render_widget(gauge, left_chunk[0]);

            let logs: String = app.logs.join("\n\n");

            let paragraph = Paragraph::new(logs)
                .style(Style::default().bg(Color::Black).fg(Color::White))
                .block(Block::default().title("Logs").borders(Borders::ALL))
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: false });
            f.render_widget(paragraph, left_chunk[1]);

            let sync_process = if app.total_blocs + app.synced_blocks > 0.0 {
                format!("{}/{}", app.synced_blocks, app.total_blocs)
            } else {
                String::from("--/--")
            };

            let paragraph = Paragraph::new(sync_process)
                .style(Style::default().bg(Color::Black).fg(Color::White))
                .block(Block::default().title("Sync Progress").borders(Borders::ALL))
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: true });
            f.render_widget(paragraph, right_chunk[0]);

            let db_stats = if let Some(stats) = &app.stats {
                format!("{:#?}", stats)
            } else {
                String::from("...")
            };

            let paragraph = Paragraph::new(db_stats)
                .style(Style::default().bg(Color::Black).fg(Color::White))
                .block(Block::default().title("Database Stats").borders(Borders::ALL))
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: false });
            f.render_widget(paragraph, right_chunk[2]);
        });
        match events.next()? {
            Event::Input(input) => {
                if input == Key::Ctrl('c') {
                    std::process::exit(0);
                }
            }
            Event::Tick => {}
        }
    }

    Ok(())
}

