extern crate merkle_storage;

use merkle_storage::prelude::*;
use std::sync::{Arc, RwLock};
use rayon::prelude::*;
use std::time::Instant;
use rand::prelude::*;
use std::sync::atomic::{AtomicI64, Ordering};
use timer;
use chrono::Duration;

fn main() {
    // simulate parallel connected clients
    let c = 100;
    // simulate set commands by clients
    let n = 50000;
    let mut vec = vec![vec!["data".to_string(), "a".to_string(), "x".to_string()]; c];
    let db = Arc::new(RwLock::new(DB::new()));
    let timer = timer::Timer::new();
    let timer2 = timer::Timer::new();
    let mut sets = Arc::new(AtomicI64::new(0));
    let mut total_duration = Arc::new(AtomicI64::new(1));

    let mut sets_clone = sets.clone();
    let mut total_duration_clone = total_duration.clone();

    let mut sets_clone_2 = sets.clone();
    let mut total_duration_clone_2 = total_duration.clone();

    let g1 = {
        timer.schedule_repeating(Duration::seconds(1), move || {
            let s = sets_clone_2.fetch_or(0, Ordering::SeqCst);
            let d = total_duration_clone_2.fetch_or(1, Ordering::SeqCst);
            println!("{} sets/sec", s / d);
        })
    };
    let g2 = {
        timer2.schedule_repeating(Duration::seconds(1), move || {
            total_duration_clone.fetch_add(1, Ordering::SeqCst);
        })
    };

    vec.par_iter().for_each(|k| {
        let mut storage = MerkleStorage::new(db.clone());
        for i in 1..n {
            let mut rng = rand::thread_rng();
            let mut v: Vec<u8> = (1..5).collect();
            v.shuffle(&mut rng);
            storage.set(k, &v);
            sets_clone.fetch_add(1, Ordering::SeqCst);
        }
        //println!("{} sets/ {} ms", n, timer.elapsed().as_millis());
    });

    drop(g1);
    drop(g2);

    let s = sets.fetch_or(0, Ordering::SeqCst);
    let d = total_duration.fetch_or(1, Ordering::SeqCst);
    println!("clients :{} reqs: {} sets: {}  finished in {} secs, {} sets/sec", c, n, s, d, s / d)
}
