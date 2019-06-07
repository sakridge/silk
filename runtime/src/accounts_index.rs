use crate::bank::PerfStats;
use log::*;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::timing::duration_as_ns;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

pub type Fork = u64;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct AccountsIndex<T> {
    #[serde(skip)]
    pub account_maps: HashMap<Pubkey, Vec<(Fork, T)>>,

    pub roots: HashSet<Fork>,

    //This value that needs to be stored to recover the index from AppendVec
    pub last_root: Fork,
}

impl<T: Clone> AccountsIndex<T> {
    /// Get an account
    /// The latest account that appears in `ancestors` or `roots` is returned.
    pub fn get(&self, pubkey: &Pubkey, ancestors: &HashMap<Fork, usize>) -> Option<(&T, Fork)> {
        let list = self.account_maps.get(pubkey)?;
        let mut max = 0;
        let mut rv = None;
        for e in list.iter().rev() {
            if e.0 >= max && (ancestors.get(&e.0).is_some() || self.is_root(e.0)) {
                trace!("GET {} {:?}", e.0, ancestors);
                rv = Some((&e.1, e.0));
                max = e.0;
            }
        }
        rv
    }

    /// Insert a new fork.
    /// @retval - The return value contains any squashed accounts that can freed from storage.
    pub fn insert(
        &mut self,
        fork: Fork,
        pubkey: &Pubkey,
        account_info: T,
        stats: &mut PerfStats,
    ) -> Vec<(Fork, T)> {
        let mut rv = vec![];
        let now = Instant::now();
        let mut fork_vec: Vec<(Fork, T)> = vec![];
        {
            let entry = self.account_maps.entry(*pubkey).or_insert_with(|| vec![]);
            std::mem::swap(entry, &mut fork_vec);
        };

        // filter out old entries
        rv.extend(fork_vec.iter().filter(|(f, _)| *f == fork).cloned());
        fork_vec.retain(|(f, _)| *f != fork);
        let sample = thread_rng().gen_range(0, 500);
        if sample == 0 {
            info!("rv.len: {} fork_vec.len: {}", rv.len(), fork_vec.len());
        }
        stats.insert1 += duration_as_ns(&now.elapsed());
        let now = Instant::now();

        // add the new entry
        fork_vec.push((fork, account_info));

        rv.extend(
            fork_vec
                .iter()
                .filter(|(fork, _)| self.is_purged(*fork))
                .cloned(),
        );

        stats.insert2 += duration_as_ns(&now.elapsed());
        let now = Instant::now();

        fork_vec.retain(|(fork, _)| !self.is_purged(*fork));
        {
            let entry = self.account_maps.entry(*pubkey).or_insert_with(|| vec![]);
            std::mem::swap(entry, &mut fork_vec);
        };

        stats.insert3 += duration_as_ns(&now.elapsed());

        rv
    }

    pub fn add_index(&mut self, fork: Fork, pubkey: &Pubkey, account_info: T) {
        let entry = self.account_maps.entry(*pubkey).or_insert_with(|| vec![]);
        entry.push((fork, account_info));
    }

    pub fn is_purged(&self, fork: Fork) -> bool {
        fork < self.last_root
    }
    pub fn is_root(&self, fork: Fork) -> bool {
        self.roots.contains(&fork)
    }
    pub fn add_root(&mut self, fork: Fork) {
        assert!(
            (self.last_root == 0 && fork == 0) || (fork > self.last_root),
            "new roots must be increasing"
        );
        self.last_root = fork;
        self.roots.insert(fork);
    }
    /// Remove the fork when the storage for the fork is freed
    /// Accounts no longer reference this fork.
    pub fn cleanup_dead_fork(&mut self, fork: Fork) {
        self.roots.remove(&fork);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::{Keypair, KeypairUtil};

    #[test]
    fn test_get_empty() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let ancestors = HashMap::new();
        assert_eq!(index.get(&key.pubkey(), &ancestors), None);
    }

    #[test]
    fn test_insert_no_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.pubkey(), true);
        assert!(gc.is_empty());

        let ancestors = HashMap::new();
        assert_eq!(index.get(&key.pubkey(), &ancestors), None);
    }

    #[test]
    fn test_insert_wrong_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.pubkey(), true);
        assert!(gc.is_empty());

        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(index.get(&key.pubkey(), &ancestors), None);
    }

    #[test]
    fn test_insert_with_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.pubkey(), true);
        assert!(gc.is_empty());

        let ancestors = vec![(0, 0)].into_iter().collect();
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&true, 0)));
    }

    #[test]
    fn test_is_root() {
        let mut index = AccountsIndex::<bool>::default();
        assert!(!index.is_root(0));
        index.add_root(0);
        assert!(index.is_root(0));
    }

    #[test]
    fn test_insert_with_root() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.pubkey(), true);
        assert!(gc.is_empty());

        let ancestors = vec![].into_iter().collect();
        index.add_root(0);
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&true, 0)));
    }

    #[test]
    fn test_is_purged() {
        let mut index = AccountsIndex::<bool>::default();
        assert!(!index.is_purged(0));
        index.add_root(1);
        assert!(index.is_purged(0));
        index.add_root(2);
        assert!(index.is_purged(1));
    }

    #[test]
    fn test_max_last_root() {
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(1);
        assert_eq!(index.last_root, 1);
    }

    #[test]
    #[should_panic]
    fn test_max_last_root_old() {
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(1);
        index.add_root(0);
    }

    #[test]
    fn test_cleanup_first() {
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(0);
        index.add_root(1);
        index.cleanup_dead_fork(0);
        assert!(index.is_root(1));
        assert!(!index.is_root(0));
    }

    #[test]
    fn test_cleanup_last() {
        //this behavior might be undefined, clean up should only occur on older forks
        let mut index = AccountsIndex::<bool>::default();
        index.add_root(0);
        index.add_root(1);
        index.cleanup_dead_fork(1);
        assert!(!index.is_root(1));
        assert!(index.is_root(0));
    }

    #[test]
    fn test_update_last_wins() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let gc = index.insert(0, &key.pubkey(), true);
        assert!(gc.is_empty());
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&true, 0)));

        let gc = index.insert(0, &key.pubkey(), false);
        assert_eq!(gc, vec![(0, true)]);
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&false, 0)));
    }

    #[test]
    fn test_update_new_fork() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let gc = index.insert(0, &key.pubkey(), true);
        assert!(gc.is_empty());
        let gc = index.insert(1, &key.pubkey(), false);
        assert!(gc.is_empty());
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&true, 0)));
        let ancestors = vec![(1, 0)].into_iter().collect();
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&false, 1)));
    }

    #[test]
    fn test_update_gc_purged_fork() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let gc = index.insert(0, &key.pubkey(), true);
        assert!(gc.is_empty());
        index.add_root(1);
        let gc = index.insert(1, &key.pubkey(), false);
        assert_eq!(gc, vec![(0, true)]);
        let ancestors = vec![].into_iter().collect();
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&false, 1)));
    }
}
