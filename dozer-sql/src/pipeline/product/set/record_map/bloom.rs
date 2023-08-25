//! Based on https://www.arunma.com/2023/03/19/build-your-own-counting-bloom-filter-in-rust/

use std::hash::Hash;

use dozer_types::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct CountingBloomFilter {
    counters: Vec<u8>,
    num_hashes: u32,
    #[serde(skip)]
    hasher: hash::BloomHasher,
}

impl CountingBloomFilter {
    pub fn with_rate(false_positive_rate: f32, expected_num_items: u32) -> Self {
        let num_counters = optimal_num_counters(expected_num_items, false_positive_rate);
        let num_hashes = optimal_num_hashes(expected_num_items, num_counters);
        Self {
            counters: vec![0; num_counters],
            num_hashes,
            hasher: Default::default(),
        }
    }

    pub fn insert<V: Hash>(&mut self, value: &V) {
        for slot in calculate_slots(&self.hasher, value, self.num_hashes, self.counters.len()) {
            self.counters[slot] = self.counters[slot].saturating_add(1);
        }
    }

    pub fn remove<V: Hash>(&mut self, value: &V) {
        let slots = calculate_slots(&self.hasher, value, self.num_hashes, self.counters.len())
            .collect::<Vec<_>>();
        if slots.iter().all(|slot| self.counters[*slot] > 0) {
            for slot in slots {
                self.counters[slot] = self.counters[slot].saturating_sub(1);
            }
        }
    }

    pub fn estimate_count<V: Hash>(&self, value: &V) -> u8 {
        calculate_slots(&self.hasher, value, self.num_hashes, self.counters.len())
            .map(|slot| self.counters[slot])
            .min()
            .unwrap_or(0)
    }

    pub fn clear(&mut self) {
        self.counters.iter_mut().for_each(|counter| *counter = 0);
    }
}

fn optimal_num_counters(num_items: u32, false_positive_rate: f32) -> usize {
    -(num_items as f32 * false_positive_rate.ln() / (2.0f32.ln().powi(2))).ceil() as usize
}

fn optimal_num_hashes(num_items: u32, num_counters: usize) -> u32 {
    let k = (num_counters as f64 / num_items as f64 * 2.0f64.ln()).round() as u32;
    if k < 1 {
        1
    } else {
        k
    }
}

fn calculate_slots<V: Hash>(
    hasher: &hash::BloomHasher,
    value: &V,
    num_hashes: u32,
    num_counters: usize,
) -> impl Iterator<Item = usize> {
    hasher
        .calculate_hashes(value, num_hashes)
        .map(move |hash| (hash % num_counters as u64) as usize)
}

mod hash {
    use std::hash::{BuildHasher, Hash};

    use ahash::RandomState;

    #[derive(Debug)]
    pub struct BloomHasher {
        random_state_1: RandomState,
        random_state_2: RandomState,
    }

    impl Default for BloomHasher {
        fn default() -> Self {
            // We're using fixed keys because `RandomState` cannot be serialized. These are just two random numbers.
            const KEY1: usize = 11636376767615148353;
            const KEY2: usize = 1474968174732524820;
            let random_state_1 = RandomState::with_seed(KEY1);
            let random_state_2 = RandomState::with_seed(KEY2);
            Self {
                random_state_1,
                random_state_2,
            }
        }
    }

    impl BloomHasher {
        pub fn calculate_hashes(
            &self,
            value: &impl Hash,
            num_hashes: u32,
        ) -> impl Iterator<Item = u64> {
            calculate_hashes_impl(
                value,
                num_hashes,
                &self.random_state_1,
                &self.random_state_2,
            )
        }
    }

    /// From paper "Less Hashing, Same Performance: Building a Better Bloom Filter"
    fn calculate_hashes_impl<T: Hash>(
        value: &T,
        num_hashes: u32,
        hasher_builder1: &impl BuildHasher,
        hasher_builder2: &impl BuildHasher,
    ) -> impl Iterator<Item = u64> {
        let hash1 = hasher_builder1.hash_one(value);
        let hash2 = hasher_builder2.hash_one(value);
        (0..num_hashes).map(move |i| hash1.wrapping_add((i as u64).wrapping_mul(hash2)))
    }
}
