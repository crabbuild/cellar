//! Bounded remembered coverage; eviction never removes durable state.
use std::collections::{BTreeMap, VecDeque};

type Key = (String, u64);

pub(crate) struct CoverageCache {
    entries: BTreeMap<Key, u64>,
    insertion_order: VecDeque<Key>,
    capacity: usize,
}

impl CoverageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            insertion_order: VecDeque::new(),
            capacity,
        }
    }

    pub fn get(&self, key: &Key) -> Option<&u64> {
        self.entries.get(key)
    }

    pub fn remember(&mut self, cell: &str, epoch: u64, txid: u64) {
        if self.capacity == 0 {
            return;
        }
        let key = (cell.to_owned(), epoch);
        if let Some(known) = self.entries.get_mut(&key) {
            *known = (*known).max(txid);
            return;
        }
        // Updating a hot key must not accumulate queue entries. FIFO avoids
        // clocks and keeps both inventories bounded, including repeated hits.
        if self.entries.len() == self.capacity {
            let oldest = self
                .insertion_order
                .pop_front()
                .expect("every cached coverage entry has one eviction key");
            self.entries.remove(&oldest);
        }
        self.insertion_order.push_back(key.clone());
        self.entries.insert(key, txid);
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updates_preserve_maximum_without_growing_eviction_metadata() {
        let mut cache = CoverageCache::new(2);
        cache.remember("Counter:a", 1, 8);
        cache.remember("Counter:a", 2, 3);
        for _ in 0..10_000 {
            cache.remember("Counter:a", 1, 4);
            assert_eq!(cache.get(&("Counter:a".into(), 1)), Some(&8));
        }
        assert_eq!(cache.insertion_order.len(), 2);
        cache.remember("Counter:b", 1, 0);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&("Counter:a".into(), 1)), None);
        assert_eq!(cache.get(&("Counter:a".into(), 2)), Some(&3));
        assert_eq!(cache.get(&("Counter:b".into(), 1)), Some(&0));
        cache.remember("Counter:a", 1, 9);
        assert_eq!(cache.get(&("Counter:a".into(), 1)), Some(&9));
        assert_eq!(cache.get(&("Counter:a".into(), 2)), None);
        assert_eq!(cache.insertion_order.len(), 2);
    }

    #[test]
    fn zero_capacity_disables_remembered_coverage() {
        let mut cache = CoverageCache::new(0);
        cache.remember("Counter:a", 1, 10);
        assert_eq!(cache.get(&("Counter:a".into(), 1)), None);
        assert!(cache.entries.is_empty() && cache.insertion_order.is_empty());
    }
}
