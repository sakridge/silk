use hash::{Hash};
use entry::Entry;
use rand::{ChaChaRng, Rng, SeedableRng};

// Block of hash answers to validate against
// Vec of [ledger blocks] x [keys]
type StorageResults = Vec<Vec<Hash>>;

pub struct StorageStage {
    storage_results: StorageResults,
    storage_keys: Vec<u8>,
}

macro_rules! cross_boundary {
    ($start:expr, $len:expr, $boundary:expr) => {
        (($start + $len) & !($boundary - 1)) > $start & !($boundary - 1)
    };
}

const NUM_HASHES_FOR_STORAGE_ROTATE: u64 = 1024;
const NUM_IDENTITIES: usize = 1024;
pub const ENTRIES_PER_SLICE: u64 = 16;
const KEY_SIZE: usize = 32;

impl StorageStage {
    pub fn new() -> Self {
        StorageStage
            {
              storage_results: vec![vec![]; NUM_IDENTITIES],
              storage_keys: vec![0u8; KEY_SIZE * NUM_IDENTITIES],
            }
    }

    pub fn process_entries(&mut self, entries: &[Entry], poh_height: &mut u64, entry_height: &mut u64) {
        for entry in entries {
            if cross_boundary!(*poh_height, entry.num_hashes, NUM_HASHES_FOR_STORAGE_ROTATE) {
                let mut seed = [0u8; 32];
                seed.copy_from_slice(entry.id.as_ref());
                let mut rng = ChaChaRng::from_seed(seed);

                // Regenerate the identity keys
                rng.fill(&mut self.storage_keys[..]);

                // Regenerate the answers
                let num_slices = (*entry_height / ENTRIES_PER_SLICE) as usize;
                for slice in 0..num_slices {
                    for identity in 0..NUM_IDENTITIES {
                        self.storage_results[slice][identity] = Hash::default();
                    }
                }
            }
            *poh_height += entry.num_hashes;
        }
        *entry_height += entries.len() as u64;
    }
}
