#![feature(test)]

extern crate test;

use log::*;
use rand::{thread_rng, Rng};
use solana_runtime::accounts_db::AccountInfo;
use solana_runtime::accounts_index::AccountsIndex;
use solana_runtime::bank::PerfStats;
use solana_sdk::pubkey::Pubkey;
use test::Bencher;
use std::collections::HashMap;

#[bench]
fn bench_accounts_index(bencher: &mut Bencher) {
    solana_logger::setup();
    const NUM_PUBKEYS: usize = 100_000;
    let pubkeys: Vec<_> = (0..NUM_PUBKEYS)
        .into_iter()
        .map(|_| Pubkey::new_rand())
        .collect();

    const NUM_FORKS: u64 = 16;

    let mut reclaims = vec![];
    let mut stats = PerfStats::default();
    let mut index = AccountsIndex::<AccountInfo>::default();
    for f in 0..NUM_FORKS {
        for _p in 0..NUM_PUBKEYS {
            index.insert(
                f,
                &pubkeys[_p],
                AccountInfo::default(),
                &mut stats,
                &mut reclaims,
            );
            reclaims.clear();
        }
    }

    let mut fork = NUM_FORKS;
    let mut root = 0;
    //let mut ancestors = HashMap::new();
    let mut zero_forks = 0;
    bencher.iter(|| {
        let mut stats = PerfStats::default();
        let mut times = 0;
        for _p in 0..NUM_PUBKEYS {
            let pubkey = thread_rng().gen_range(0, NUM_PUBKEYS);
            index.insert(
                fork,
                &pubkeys[pubkey],
                AccountInfo::default(),
                &mut stats,
                &mut reclaims,
            );
            reclaims.clear();
            /*if index.get(&pubkeys[pubkey], &ancestors).is_some() {
                zero_forks += 1;
            }*/
        }
        index.add_root(root);
        root += 1;
        fork += 1;
        //println!("times: {} stats: {:?}", times, stats);
    });
    println!("zero_forks: {}", zero_forks);
}
