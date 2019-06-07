#![feature(test)]

extern crate test;

use log::*;
use rand::{thread_rng, Rng};
use solana_runtime::accounts_db::AccountInfo;
use solana_runtime::accounts_index::AccountsIndex;
use solana_runtime::bank::PerfStats;
use solana_sdk::pubkey::Pubkey;
use test::Bencher;

#[bench]
fn bench_accounts_index(bencher: &mut Bencher) {
    solana_logger::setup();
    const NUM_PUBKEYS: usize = 2;
    let pubkeys: Vec<_> = (0..NUM_PUBKEYS)
        .into_iter()
        .map(|_| Pubkey::new_rand())
        .collect();

    const NUM_FORKS: u64 = 32;
    bencher.iter(|| {
        let mut stats = PerfStats::default();
        let mut index = AccountsIndex::<AccountInfo>::default();
        for x in 0..2 {
            for fork in 0..NUM_FORKS {
                let pubkey = thread_rng().gen_range(0, NUM_PUBKEYS);
                index.insert(fork, &pubkeys[pubkey], AccountInfo::default(), &mut stats);
                //index.insert(fork, pubkey, AccountInfo::default());
            }
            //index.add_root(x * 4);
        }
        //println!("stats: {:?}", stats);
    });
}
