use dozer_types::{
    errors::types::DeserializationError,
    serde::{Deserialize, Serialize},
    types::Record,
};
use enum_dispatch::enum_dispatch;
use std::collections::HashMap;

#[enum_dispatch(CountingRecordMap)]
pub enum CountingRecordMapEnum {
    AccurateCountingRecordMap,
    ProbabilisticCountingRecordMap,
}

#[enum_dispatch]
pub trait CountingRecordMap {
    /// Inserts a record, or increases its insertion count if it already exixts in the map.
    fn insert(&mut self, record: &Record);

    /// Decreases the insertion count of a record, and removes it if the count reaches zero.
    fn remove(&mut self, record: &Record);

    /// Returns an estimate of the number of times this record has been inserted into the filter.
    /// Depending on the implementation, this number may not be accurate.
    fn estimate_count(&self, record: &Record) -> u64;

    /// Clears the map, removing all records.
    fn clear(&mut self);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccurateCountingRecordMap {
    map: HashMap<Record, u64>,
}

impl AccurateCountingRecordMap {
    pub fn new() -> Result<Self, DeserializationError> {
        Ok(Self {
            map: Default::default(),
        })
    }
}

impl CountingRecordMap for AccurateCountingRecordMap {
    fn insert(&mut self, record: &Record) {
        let count = self.map.entry(record.clone()).or_insert(0);
        if *count < u64::max_value() {
            *count += 1;
        }
    }

    fn remove(&mut self, record: &Record) {
        if let Some(count) = self.map.get_mut(record) {
            *count -= 1;
            if *count == 0 {
                self.map.remove(record);
            }
        }
    }

    fn estimate_count(&self, record: &Record) -> u64 {
        self.map.get(record).copied().unwrap_or(0)
    }

    fn clear(&mut self) {
        self.map.clear();
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct ProbabilisticCountingRecordMap {
    map: bloom::CountingBloomFilter,
}

impl ProbabilisticCountingRecordMap {
    const FALSE_POSITIVE_RATE: f32 = 0.01;
    const EXPECTED_NUM_ITEMS: u32 = 10000000;

    pub fn new() -> Result<Self, DeserializationError> {
        Ok(Self {
            map: bloom::CountingBloomFilter::with_rate(
                Self::FALSE_POSITIVE_RATE,
                Self::EXPECTED_NUM_ITEMS,
            ),
        })
    }
}

impl CountingRecordMap for ProbabilisticCountingRecordMap {
    fn insert(&mut self, record: &Record) {
        self.map.insert(record);
    }

    fn remove(&mut self, record: &Record) {
        self.map.remove(record);
    }

    fn estimate_count(&self, record: &Record) -> u64 {
        self.map.estimate_count(record) as u64
    }

    fn clear(&mut self) {
        self.map.clear();
    }
}

mod bloom;

#[cfg(test)]
mod tests {
    use dozer_types::types::{Field, Record};

    use super::{
        AccurateCountingRecordMap, CountingRecordMap, CountingRecordMapEnum,
        ProbabilisticCountingRecordMap,
    };

    fn test_map(mut map: CountingRecordMapEnum) {
        let make_record = Record::new;

        let a = make_record(vec![Field::String('a'.into())]);
        let b = make_record(vec![Field::String('b'.into())]);

        assert_eq!(map.estimate_count(&a), 0);
        assert_eq!(map.estimate_count(&b), 0);

        map.insert(&a);
        map.insert(&b);
        assert_eq!(map.estimate_count(&a), 1);
        assert_eq!(map.estimate_count(&b), 1);

        map.insert(&b);
        map.insert(&b);
        assert_eq!(map.estimate_count(&a), 1);
        assert_eq!(map.estimate_count(&b), 3);

        map.remove(&b);
        assert_eq!(map.estimate_count(&a), 1);
        assert_eq!(map.estimate_count(&b), 2);

        map.remove(&a);
        assert_eq!(map.estimate_count(&a), 0);
        assert_eq!(map.estimate_count(&b), 2);

        map.clear();
        assert_eq!(map.estimate_count(&a), 0);
        assert_eq!(map.estimate_count(&b), 0);
    }

    #[test]
    fn test_maps() {
        let accurate_map = AccurateCountingRecordMap::new().unwrap().into();
        test_map(accurate_map);

        let probabilistic_map = ProbabilisticCountingRecordMap::new().unwrap().into();
        test_map(probabilistic_map);
    }
}
