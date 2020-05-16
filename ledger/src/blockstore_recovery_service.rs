use crate::blockstore::Blockstore;
use crate::leader_schedule_cache::LeaderScheduleCache;
use crate::shred::Shred;
use solana_measure::measure::Measure;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;

pub struct BlockstoreRecoveryService {
    t_recovery: JoinHandle<()>,
}

impl BlockstoreRecoveryService {
    pub fn new(
        leader_schedule: Option<&Arc<LeaderScheduleCache>>,
        blockstore: Arc<Blockstore>,
        shred_receiver: Receiver<(HashMap<(u64, u64), Shred>, HashMap<(u64, u64), Shred>)>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let exit = exit.clone();
        let leader_schedule = leader_schedule.cloned();
        let t_recovery = Builder::new()
            .name("solana-ledger-recovery".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                let timeout = Duration::from_secs(1);
                if let Ok(shreds_received) = shred_receiver.recv_timeout(timeout) {
                    let (just_inserted_data_shreds, just_inserted_coding_shreds) = shreds_received;
                    Self::handle_recovery(
                        leader_schedule.as_ref(),
                        blockstore,
                        just_inserted_data_shreds,
                        just_inserted_coding_shreds,
                    );
                }
            })
            .unwrap();
        Self { t_recovery }
    }

    fn handle_recovery(
        leader_schedule: Option<&Arc<LeaderScheduleCache>>,
        blockstore: Arc<Blockstore>,
        just_inserted_data_shreds: HashMap<(u64, u64), Shred>,
        just_inserted_coding_shreds: HashMap<(u64, u64), Shred>,
    ) {
        let mut index_working_set = HashMap::new();
        let mut start = Measure::start("shred recovery");
        if let Some(leader_schedule_cache) = leader_schedule {
            let recovered_data = blockstore.try_shred_recovery(
                &mut index_working_set,
                &mut just_inserted_data_shreds,
                &mut just_inserted_coding_shreds,
            );

            let num_recovered = recovered_data.len();
            let verified_shreds = recovered_data
                .into_iter()
                .filter_map(|shred| {
                    if let Some(leader) = leader_schedule_cache.slot_leader_at(shred.slot(), None) {
                        if shred.verify(&leader) {
                            Some(shred)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            blockstore.insert_shreds_handle_duplicate(
                verified_shreds,
                &mut erasure_metas,
                &mut index_working_set,
                &mut slot_meta_working_set,
                &mut write_batch,
                &mut just_inserted_data_shreds,
                &mut index_meta_time,
                is_trusted,
                &handle_duplicate,
            );
        }
        start.stop();
        let shred_recovery_elapsed = start.as_us();

    }
}
