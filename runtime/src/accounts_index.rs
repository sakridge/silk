use crate::bank::PerfStats;
use log::*;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::timing::duration_as_ns;
use std::collections::VecDeque;
use std::collections;
use std::fmt::Debug;
use std::time::Instant;
use hashbrown::{HashMap, HashSet};
use std::hash::BuildHasherDefault;

pub type Fork = u64;

//type HashMapFnv<T> = HashMap<Fork, T, BuildHasherDefault<FnvHasher>>;
#[cfg(feature = "hashmap")]
type HashMapFnv<T> = HashMap<Fork, T>;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct AccountsIndex<T> {
    #[serde(skip)]
    #[cfg(feature = "hashmap")]
    pub account_maps: collections::HashMap<Pubkey, (Vec<Fork>, HashMapFnv<T>)>,

    #[serde(skip)]
    #[cfg(feature = "vec")]
    pub account_maps: HashMap<Pubkey, Vec<(Fork, T)>>,

    #[serde(skip)]
    #[cfg(feature = "hashset")]
    pub account_maps: HashMap<Pubkey, (Vec<(Fork, T)>, HashSet<Fork>)>,

    pub roots: collections::HashSet<Fork>,

    //This value that needs to be stored to recover the index from AppendVec
    pub last_root: Fork,
}

impl<T: Clone + Debug> AccountsIndex<T> {
    /// Get an account
    /// The latest account that appears in `ancestors` or `roots` is returned.

    #[cfg(feature = "hashset")]
    pub fn get(&self, pubkey: &Pubkey, ancestors: &collections::HashMap<Fork, usize>) -> Option<(&T, Fork)> {
        let list = self.account_maps.get(pubkey)?;
        let mut max = 0;
        let mut rv = None;
        for e in list.0.iter().rev() {
            if e.0 >= max && (ancestors.get(&e.0).is_some() || self.is_root(e.0)) {
                trace!("GET {} {:?}", e.0, ancestors);
                rv = Some((&e.1, e.0));
                max = e.0;
            }
        }
        rv
    }

    #[cfg(feature = "vec")]
    pub fn get(&self, pubkey: &Pubkey, ancestors: &collections::HashMap<Fork, usize>) -> Option<(&T, Fork)> {
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

    #[cfg(feature = "hashmap")]
    pub fn get(&self, pubkey: &Pubkey, ancestors: &collections::HashMap<Fork, usize>) -> Option<(&T, Fork)> {
        let list = self.account_maps.get(pubkey)?;
        let mut max = 0;
        let mut found_max = false;
        for e in list.0.iter().rev() {
            if *e >= max && (ancestors.get(e).is_some() || self.is_root(*e)) {
                trace!("GET {} {:?}", *e, ancestors);
                max = *e;
                found_max = true;
            }
        }
        if found_max {
            Some((list.1.get(&max).unwrap(), max))
        } else {
            None
        }
    }

    // Vec<(Fork, T)>
    #[cfg(feature = "vec")]
    pub fn insert(
        &mut self,
        fork: Fork,
        pubkey: &Pubkey,
        account_info: T,
        stats: &mut PerfStats,
        reclaims: &mut Vec<(Fork, T)>,
    ) {
        stats.insert_count += 1;
        //let now = Instant::now();

        let last_root = self.last_root;
        let fork_vec = self
            .account_maps
            .entry(*pubkey)
            .or_insert_with(|| (Vec::with_capacity(32)));

        // filter out old entries
        reclaims.extend(fork_vec.iter().filter(|(f, _)| *f == fork).cloned());
        fork_vec.retain(|(f, _)| *f != fork);

        //stats.insert1 += duration_as_ns(&now.elapsed());
        //let now = Instant::now();

        // add the new entry
        fork_vec.push((fork, account_info));

        reclaims.extend(
            fork_vec
                .iter()
                .filter(|(fork, _)| Self::is_purged(last_root, *fork))
                .cloned(),
        );

        //stats.insert2 += duration_as_ns(&now.elapsed());
        //let now = Instant::now();

        fork_vec.retain(|(fork, _)| !Self::is_purged(last_root, *fork));

        /*if rv.len() > 0 {
            stats.rv_gt_0 += 1;
        }
        if rv.len() > 1 {
            stats.rv_gt_1 += 1;
        }
        stats.insert3 += duration_as_ns(&now.elapsed());*/
    }

    // Vec<(Fork, T)>, HashSet<T>
    #[cfg(feature = "hashset")]
    pub fn insert(
        &mut self,
        fork: Fork,
        pubkey: &Pubkey,
        account_info: T,
        stats: &mut PerfStats,
    ) -> Vec<(Fork, T)> {
        stats.insert_count += 1;
        let mut rv = Vec::with_capacity(2);
        //let now = Instant::now();

        let last_root = self.last_root;
        let fork_vec = self
            .account_maps
            .entry(*pubkey)
            .or_insert_with(|| (Vec::with_capacity(32), HashSet::new()));

        //stats.insert1 += duration_as_ns(&now.elapsed());
        //let now = Instant::now();

        if fork_vec.1.contains(&fork) {
            stats.contains += 1;
            rv.extend(fork_vec.0.iter().filter(|(f, _)| *f == fork).cloned());
            fork_vec.0.retain(|(f, _)| *f != fork);
        } else {
            stats.no_contains += 1;
            fork_vec.1.insert(fork);
        }

        //stats.insert2 += duration_as_ns(&now.elapsed());
        //let now = Instant::now();

        // add the new entry
        fork_vec.0.push((fork, account_info));

        let mut has_purged = false;
        fork_vec.1.retain(|fork| {
            let is_purged = Self::is_purged(last_root, *fork);
            has_purged |= is_purged;
            !is_purged
        });
        if has_purged {
            let purged_entries = fork_vec
                .0
                .iter()
                .filter(|(fork, _)| Self::is_purged(last_root, *fork))
                .cloned();
            rv.extend(purged_entries);
        }

        /*if rv.len() > 0 {
            stats.rv_gt_0 += 1;
        }
        if rv.len() > 1 {
            stats.rv_gt_1 += 1;
        }
        stats.insert3 += duration_as_ns(&now.elapsed());
        */
        rv
    }

    /// Insert a new fork.
    /// @retval - The return value contains any squashed accounts that can freed from storage.
    #[cfg(feature = "hashmap")]
    pub fn insert(
        &mut self,
        fork: Fork,
        pubkey: &Pubkey,
        account_info: T,
        stats: &mut PerfStats,
    ) -> Vec<(Fork, T)> {
        stats.insert_count += 1;
        let mut rv = vec![]; //Vec::with_capacity(2);
        //let now = Instant::now();

        let last_root = self.last_root;
        let fork_vec = self
            .account_maps
            .entry(*pubkey)
            .or_insert_with(|| {
                //let mut map = HashMap::new();
                let mut map = HashMapFnv::default();
                //map.reserve(32);
                //(Vec::with_capacity(32), map)
                (Vec::with_capacity(16), map)
            });

        /*stats.insert1 += duration_as_ns(&now.elapsed());
        let now = Instant::now();*/

        if let Some(info) = fork_vec.1.remove(&fork) {
            stats.contains += 1;
            rv.push((fork, info));
        } else {
            stats.no_contains += 1;
            fork_vec.0.push(fork);
        }
        fork_vec.1.insert(fork, account_info);

        /*stats.insert2 += duration_as_ns(&now.elapsed());
        let now = Instant::now();*/

        let mut purged = vec![]; //Vec::with_capacity(2);
        fork_vec.0.retain(|fork| {
            let is_purged = Self::is_purged(last_root, *fork);
            if is_purged {
                purged.push(*fork);
            }
            !is_purged
        });

        for purge in &purged {
            rv.push((*purge, fork_vec.1.remove(purge).unwrap()));
        }

        /*if rv.len() > 0 {
            stats.rv_gt_0 += 1;
        }
        if rv.len() > 1 {
            stats.rv_gt_1 += 1;
        }
        stats.insert3 += duration_as_ns(&now.elapsed());*/
        rv
    }

    #[cfg(feature = "hashset")]
    pub fn add_index(&mut self, fork: Fork, pubkey: &Pubkey, account_info: T) {
        let entry = self
            .account_maps
            .entry(*pubkey)
            .or_insert_with(|| (Vec::new(), HashSet::new()));
        entry.0.push((fork, account_info));
        entry.1.insert(fork);
    }

    #[cfg(feature = "vec")]
    pub fn add_index(&mut self, fork: Fork, pubkey: &Pubkey, account_info: T) {
        let entry = self
            .account_maps
            .entry(*pubkey)
            .or_insert_with(|| (Vec::new()));
        entry.push((fork, account_info));
    }

    #[cfg(feature = "hashmap")]
    pub fn add_index(&mut self, fork: Fork, pubkey: &Pubkey, account_info: T) {
        let entry = self
            .account_maps
            .entry(*pubkey)
            .or_insert_with(|| (Vec::new(), HashMapFnv::default()));
        entry.0.push(fork);
        entry.1.insert(fork, account_info);
    }

    pub fn is_purged(last_root: Fork, fork: Fork) -> bool {
        fork < last_root
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
        let ancestors = collections::HashMap::new();
        assert_eq!(index.get(&key.pubkey(), &ancestors), None);
    }

    #[test]
    fn test_insert_no_ancestors() {
        solana_logger::setup();
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), true, &mut PerfStats::default(), &mut gc);
        assert!(gc.is_empty());

        let ancestors = collections::HashMap::new();
        assert_eq!(index.get(&key.pubkey(), &ancestors), None);
    }

    #[test]
    fn test_insert_wrong_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), true, &mut PerfStats::default(), &mut gc);
        assert!(gc.is_empty());

        let ancestors = vec![(1, 1)].into_iter().collect();
        assert_eq!(index.get(&key.pubkey(), &ancestors), None);
    }

    #[test]
    fn test_insert_with_ancestors() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), true, &mut PerfStats::default(), &mut gc);
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
        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), true, &mut PerfStats::default(), &mut gc);
        assert!(gc.is_empty());

        let ancestors = vec![].into_iter().collect();
        index.add_root(0);
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&true, 0)));
    }

    #[test]
    fn test_is_purged() {
        let mut index = AccountsIndex::<bool>::default();
        assert!(!AccountsIndex::<bool>::is_purged(index.last_root, 0));
        index.add_root(1);
        assert!(AccountsIndex::<bool>::is_purged(index.last_root, 0));
        index.add_root(2);
        assert!(AccountsIndex::<bool>::is_purged(index.last_root, 1));
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
        solana_logger::setup();
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), true, &mut PerfStats::default(), &mut gc);
        assert!(gc.is_empty());
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&true, 0)));

        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), false, &mut PerfStats::default(), &mut gc);
        assert_eq!(gc, vec![(0, true)]);
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&false, 0)));
    }

    #[test]
    fn test_update_new_fork() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), true, &mut PerfStats::default(), &mut gc);
        assert!(gc.is_empty());
        index.insert(1, &key.pubkey(), false, &mut PerfStats::default(), &mut gc);
        assert!(gc.is_empty());
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&true, 0)));
        let ancestors = vec![(1, 0)].into_iter().collect();
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&false, 1)));
    }

    #[test]
    fn test_update_gc_purged_fork() {
        let key = Keypair::new();
        let mut index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.insert(0, &key.pubkey(), true, &mut PerfStats::default(), &mut gc);
        assert!(gc.is_empty());
        index.add_root(1);
        index.insert(1, &key.pubkey(), false, &mut PerfStats::default(), &mut gc);
        assert_eq!(gc, vec![(0, true)]);
        let ancestors = vec![].into_iter().collect();
        assert_eq!(index.get(&key.pubkey(), &ancestors), Some((&false, 1)));
    }
}
