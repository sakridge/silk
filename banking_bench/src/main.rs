#[macro_use]
extern crate solana;
extern crate crossbeam_channel;

use solana::bank_forks::BankForks;
use crossbeam_channel::unbounded;
use solana::poh_recorder::PohRecorder;
use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use solana::banking_stage::{create_test_recorder, BankingStage};
use solana::blocktree::{get_tmp_ledger_path, Blocktree};
use solana::cluster_info::ClusterInfo;
use solana::cluster_info::Node;
use solana::genesis_utils::{create_genesis_block, GenesisBlockInfo};
use solana::packet::to_packets_chunked;
use solana::poh_recorder::WorkingBankEntries;
use solana::service::Service;
use solana_runtime::bank::Bank;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signature::Signature;
use solana_sdk::system_transaction;
use solana_sdk::timing::{duration_as_us, timestamp};
use solana_sdk::transaction::Transaction;
use std::iter;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant};


fn check_txs(receiver: &Arc<Receiver<WorkingBankEntries>>, ref_tx_count: usize, poh_recorder: &Arc<Mutex<PohRecorder>>) -> bool {
    let mut total = 0;
    let now = Instant::now();
    let mut no_bank = false;
    loop {
        let entries = receiver.recv_timeout(Duration::from_millis(100));
        if let Ok((_, entries)) = entries {
            for (entry, _) in &entries {
                total += entry.transactions.len();
            }
        }
        if total >= ref_tx_count {
            break;
        }
        if now.elapsed().as_secs() > 60 {
            break;
        }
        if poh_recorder.lock().unwrap().bank().is_none() {
            info!("no bank");
            no_bank = true;
            break;
        }
    }
    if !no_bank {
        assert!(total >= ref_tx_count);
    }
    no_bank
}

fn make_accounts_txs(txes: usize, mint_keypair: &Keypair, hash: Hash) -> Vec<Transaction> {
    let to_pubkey = Pubkey::new_rand();
    let dummy = system_transaction::transfer(mint_keypair, &to_pubkey, 1, hash);
    (0..txes)
        .into_par_iter()
        .map(|_| {
            let mut new = dummy.clone();
            let sig: Vec<u8> = (0..64).map(|_| thread_rng().gen()).collect();
            new.message.account_keys[0] = Pubkey::new_rand();
            new.message.account_keys[1] = Pubkey::new_rand();
            new.signatures = vec![Signature::new(&sig[0..64])];
            new
        })
        .collect()
}



fn main() {
    solana_logger::setup();
    let num_threads = BankingStage::num_threads() as usize;
    //   a multiple of packet chunk duplicates to avoid races
    const CHUNKS: usize = 32;
    const PACKETS_PER_BATCH: usize = 192;
    let txes = PACKETS_PER_BATCH * num_threads * CHUNKS;
    let mint_total = 1_000_000_000_000;
    let GenesisBlockInfo {
        genesis_block,
        mint_keypair,
        ..
    } = create_genesis_block(mint_total);

    let (verified_sender, verified_receiver) = unbounded();
    let (vote_sender, vote_receiver) = unbounded();
    let bank0 = Bank::new(&genesis_block);
    let mut bank_forks = BankForks::new(0, bank0);
    let mut bank = bank_forks.working_bank();

    debug!("threads: {} txs: {}", num_threads, txes);

    let transactions = make_accounts_txs(txes, &mint_keypair, genesis_block.hash());

    // fund all the accounts
    transactions.iter().for_each(|tx| {
        let fund = system_transaction::transfer(
            &mint_keypair,
            &tx.message.account_keys[0],
            mint_total / txes as u64,
            genesis_block.hash(),
        );
        let x = bank.process_transaction(&fund);
        x.unwrap();
    });
    //sanity check, make sure all the transactions can execute sequentially
    transactions.iter().for_each(|tx| {
        let res = bank.process_transaction(&tx);
        assert!(res.is_ok(), "sanity test transactions");
    });
    bank.clear_signatures();
    //sanity check, make sure all the transactions can execute in parallel
    let res = bank.process_transactions(&transactions);
    for r in res {
        assert!(r.is_ok(), "sanity parallel execution");
    }
    bank.clear_signatures();
    let verified: Vec<_> = to_packets_chunked(&transactions.clone(), PACKETS_PER_BATCH)
        .into_iter()
        .map(|x| {
            let len = x.packets.len();
            (x, iter::repeat(1).take(len).collect())
        })
        .collect();
    let ledger_path = get_tmp_ledger_path!();
    {
        let blocktree = Arc::new(
            Blocktree::open(&ledger_path).expect("Expected to be able to open database ledger"),
        );
        let (exit, poh_recorder, poh_service, signal_receiver) =
            create_test_recorder(&bank, &blocktree);
        let cluster_info = ClusterInfo::new_with_invalid_keypair(Node::new_localhost().info);
        let cluster_info = Arc::new(RwLock::new(cluster_info));
        let _banking_stage = BankingStage::new(
            &cluster_info,
            &poh_recorder,
            verified_receiver,
            vote_receiver,
        );
        poh_recorder.lock().unwrap().set_bank(&bank);

        let chunk_len = verified.len() / CHUNKS;
        let mut start = 0;

        // This is so that the signal_receiver does not go out of scope after the closure.
        // If it is dropped before poh_service, then poh_service will error when
        // calling send() on the channel.
        let signal_receiver = Arc::new(signal_receiver);
        let signal_receiver2 = signal_receiver.clone();
        let mut total = 0;
        let mut root = 1;
        const ITERS: usize = 3_000;
        for _ in 0..ITERS {
            let now = Instant::now();
            let mut sent = 0;

            for v in verified[start..start + chunk_len].chunks(chunk_len / num_threads) {
                debug!(
                    "sending... {}..{} {} v.len: {}",
                    start,
                    start + chunk_len,
                    timestamp(),
                    v.len(),
                );
                for xv in v {
                    sent += xv.0.packets.len();
                }
                verified_sender.send(v.to_vec()).unwrap();
            }
            if check_txs(&signal_receiver2, txes / CHUNKS, &poh_recorder) {
                info!("resetting bank {}", bank.slot());
                let new_bank = Bank::new_from_parent(&bank, &Pubkey::default(), bank.slot() + 1);
                bank_forks.insert(new_bank);
                bank = bank_forks.working_bank();
                poh_recorder.lock().unwrap().reset(
                                  bank.tick_height(),
                                  bank.hash(),
                                  bank.slot(),
                                  Some(bank.slot()),
                                  bank.ticks_per_slot(),
                                  );
                poh_recorder.lock().unwrap().set_bank(&bank);
                if bank.slot() > 32 {
                    bank_forks.set_root(root);
                    root += 1;
                }
            }

            // This signature clear may not actually clear the signatures
            // in this chunk, but since we rotate between CHUNKS then
            // we should clear them by the time we come around again to re-use that chunk.
            bank.clear_signatures();
            total += duration_as_us(&now.elapsed());
            info!(
                "time: {} us checked: {} sent: {}",
                duration_as_us(&now.elapsed()),
                txes / CHUNKS,
                sent,
            );
            start += chunk_len;
            start %= verified.len();
        }
        info!("average: {}", total / ITERS as u64);
        drop(vote_sender);
        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        info!("waited for poh_service");
    }
    let _unused = Blocktree::destroy(&ledger_path);
}

