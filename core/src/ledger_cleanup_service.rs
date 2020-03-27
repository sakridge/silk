//! The `ledger_cleanup_service` drops older ledger data to limit disk space usage

use solana_ledger::blockstore::Blockstore;
use solana_metrics::datapoint_debug;
use solana_sdk::clock::Slot;
use std::string::ToString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::thread;
use std::thread::{Builder, JoinHandle};
use std::time::{Instant, Duration};
use solana_measure::measure::Measure;

// This is chosen to allow enough time for
// - To try and keep the RocksDB size under 512GB at 50k tps (100 slots take ~2GB).
// - A validator to download a snapshot from a peer and boot from it
// - To make sure that if a validator needs to reboot from its own snapshot, it has enough slots locally
//   to catch back up to where it was when it stopped
pub const DEFAULT_MAX_LEDGER_SLOTS: u64 = 270_000;
// Remove a fixed number of slots at a time, it's more efficient than doing it one-by-one
pub const DEFAULT_PURGE_BATCH_SIZE: u64 = 512;

pub struct LedgerCleanupService {
    t_cleanup: JoinHandle<()>,
}

impl LedgerCleanupService {
    pub fn new(
        new_root_receiver: Receiver<Slot>,
        blockstore: Arc<Blockstore>,
        max_ledger_slots: u64,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        info!(
            "LedgerCleanupService active. Max Ledger Slots {}",
            max_ledger_slots
        );
        let exit = exit.clone();
        let mut roots_since_purge = 0;
        let t_cleanup = Builder::new()
            .name("solana-ledger-cleanup".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(e) = Self::cleanup_ledger(
                    &new_root_receiver,
                    &blockstore,
                    max_ledger_slots,
                    &mut roots_since_purge,
                    DEFAULT_PURGE_BATCH_SIZE,
                    no_rocksdb_compaction,
                ) {
                    match e {
                        RecvTimeoutError::Disconnected => break,
                        RecvTimeoutError::Timeout => (),
                    }
                }
            })
            .unwrap();
        Self { t_cleanup }
    }

    fn cleanup_ledger(
        new_root_receiver: &Receiver<Slot>,
        blockstore: &Arc<Blockstore>,
        max_ledger_utilization: u64,
        roots_since_purge: &mut u64,
        purge_batch_size: u64,
        no_rocksdb_compaction,
    ) -> Result<(), RecvTimeoutError> {
        let mut root = new_root_receiver.recv_timeout(Duration::from_secs(1))?;
        *roots_since_purge += 1;
        // Get the newest root
        while let Ok(new_root) = new_root_receiver.try_recv() {
            root = new_root;
            *roots_since_purge += 1;
        }

        if *roots_since_purge > purge_batch_size {
            let disk_utilization_pre = blockstore.storage_size();

            // Notify blockstore of impending purge
            //cleanup
            let mut num_root_slots = 0;
            let mut root_shreds = Vec::new();
            let mut iterate_time = Measure::start("iterate_time");
            let mut non_root_purged = 0;
            for (slot, meta) in blockstore.slot_meta_iterator(0).unwrap() {
                if blockstore.is_dead(slot) {
                    non_root_purged += 1;
                    blockstore.purge_slots(slot, Some(slot + 1), false);
                } else if !blockstore.is_root(slot) {
                    non_root_purged += 1;
                    blockstore.purge_slots(slot, Some(slot + 1), false);
                } else if !blockstore.is_full(slot) {
                    non_root_purged += 1;
                    blockstore.purge_slots(slot, Some(slot + 1), false);
                } else if blockstore.is_root(slot) {
                    root_shreds.push((slot, meta.completed_data_indexes.len()));
                }
                if slot > root {
                    break;
                }
            }
            info!("checking for ledger purge: {:?} max: {} slots: {} non_root_purged: {}",
                disk_utilization_pre, max_ledger_utilization, root_shreds.len(), non_root_purged);
            iterate_time.stop();
            let mut total_shreds = 0;
            let mut lowest_slot_to_clean = root_shreds[0].0;
            for (slot, num_shreds) in root_shreds.reverse() {
                if total_shreds > max_ledger_utilization {
                    lowest_slot_to_clean = slot;
                    break;
                }
            }
            let mut slot_update_time = Measure::start("slot_update");
            *blockstore.lowest_cleanup_slot.write().unwrap() = lowest_slot_to_clean;
            slot_update_time.stop();
            let mut clean_time = Measure::start("ledger_clean");
            blockstore.purge_slots(0, Some(lowest_slot_to_clean), !no_rocksdb_compaction);
            clean_time.stop();

            debug!("ledger purge: {} {} {}", iterate_time, slot_update, clean_time);

            let disk_utilization_post = blockstore.storage_size();

            if let (Ok(disk_utilization_pre), Ok(disk_utilization_post)) =
                (disk_utilization_pre, disk_utilization_post)
            {
                datapoint_debug!(
                    "ledger_disk_utilization",
                    ("disk_utilization_pre", disk_utilization_pre as i64, i64),
                    ("disk_utilization_post", disk_utilization_post as i64, i64),
                    (
                        "disk_utilization_delta",
                        (disk_utilization_pre as i64 - disk_utilization_post as i64),
                        i64
                    )
                );
            }
        }

        Ok(())
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_cleanup.join()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use solana_ledger::blockstore::make_many_slot_entries;
    use solana_ledger::get_tmp_ledger_path;
    use std::sync::mpsc::channel;

    #[test]
    fn test_cleanup() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Blockstore::open(&blockstore_path).unwrap();
        let (shreds, _) = make_many_slot_entries(0, 50, 5);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        let blockstore = Arc::new(blockstore);
        let (sender, receiver) = channel();

        //send a signal to kill slots 0-40
        let mut next_purge_slot = 0;
        sender.send(50).unwrap();
        LedgerCleanupService::cleanup_ledger(&receiver, &blockstore, 10, &mut next_purge_slot)
            .unwrap();

        //check that 0-40 don't exist
        blockstore
            .slot_meta_iterator(0)
            .unwrap()
            .for_each(|(slot, _)| assert!(slot > 40));

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_compaction() {
        let blockstore_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&blockstore_path).unwrap());

        let n = 10_000;
        let batch_size = 100;
        let batches = n / batch_size;
        let max_ledger_slots = 100;

        for i in 0..batches {
            let (shreds, _) = make_many_slot_entries(i * batch_size, batch_size, 1);
            blockstore.insert_shreds(shreds, None, false).unwrap();
        }

        let u1 = blockstore.storage_size().unwrap() as f64;

        // send signal to cleanup slots
        let (sender, receiver) = channel();
        sender.send(n).unwrap();
        let mut next_purge_batch = 0;
        LedgerCleanupService::cleanup_ledger(
            &receiver,
            &blockstore,
            max_ledger_slots,
            &mut next_purge_batch,
        )
        .unwrap();

        thread::sleep(Duration::from_secs(2));

        let u2 = blockstore.storage_size().unwrap() as f64;

        assert!(u2 < u1, "insufficient compaction! pre={},post={}", u1, u2,);

        // check that early slots don't exist
        let max_slot = n - max_ledger_slots;
        blockstore
            .slot_meta_iterator(0)
            .unwrap()
            .for_each(|(slot, _)| assert!(slot > max_slot));

        drop(blockstore);
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }
}
