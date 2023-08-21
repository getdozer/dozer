use dozer_core::{
    dozer_log::storage::Object,
    processor_record::{ProcessorRecord, ProcessorRecordStore},
};
use dozer_types::serde::{Deserialize, Serialize};
use enum_dispatch::enum_dispatch;
use std::collections::HashMap;

use crate::pipeline::utils::serialize::{
    serialize_bincode, serialize_u64, serialize_vec_u8, SerializationError,
};

#[enum_dispatch(CountingRecordMap)]
pub enum CountingRecordMapEnum {
    AccurateCountingRecordMap,
    ProbabilisticCountingRecordMap,
}

#[enum_dispatch]
pub trait CountingRecordMap {
    /// Inserts a record, or increases its insertion count if it already exixts in the map.
    fn insert(&mut self, record: &ProcessorRecord);

    /// Decreases the insertion count of a record, and removes it if the count reaches zero.
    fn remove(&mut self, record: &ProcessorRecord);

    /// Returns an estimate of the number of times this record has been inserted into the filter.
    /// Depending on the implementation, this number may not be accurate.
    fn estimate_count(&self, record: &ProcessorRecord) -> u64;

    /// Clears the map, removing all records.
    fn clear(&mut self);

    /// Serializes the map to a `Object`. `ProcessorRecord`s should be serialized as an `u64`.
    fn serialize(
        &self,
        record_store: &ProcessorRecordStore,
        object: &mut Object,
    ) -> Result<(), SerializationError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccurateCountingRecordMap {
    map: HashMap<ProcessorRecord, u64>,
}

impl AccurateCountingRecordMap {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

impl CountingRecordMap for AccurateCountingRecordMap {
    fn insert(&mut self, record: &ProcessorRecord) {
        let count = self.map.entry(record.clone()).or_insert(0);
        if *count < u64::max_value() {
            *count += 1;
        }
    }

    fn remove(&mut self, record: &ProcessorRecord) {
        if let Some(count) = self.map.get_mut(record) {
            *count -= 1;
            if *count == 0 {
                self.map.remove(record);
            }
        }
    }

    fn estimate_count(&self, record: &ProcessorRecord) -> u64 {
        self.map.get(record).copied().unwrap_or(0)
    }

    fn clear(&mut self) {
        self.map.clear();
    }

    fn serialize(
        &self,
        record_store: &ProcessorRecordStore,
        object: &mut Object,
    ) -> Result<(), SerializationError> {
        serialize_u64(self.map.len() as u64, object)?;
        for (key, value) in &self.map {
            let record = record_store.serialize_record(key)?;
            serialize_vec_u8(&record, object)?;
            serialize_u64(*value, object)?;
        }
        Ok(())
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

    pub fn new() -> Self {
        Self {
            map: bloom::CountingBloomFilter::with_rate(
                Self::FALSE_POSITIVE_RATE,
                Self::EXPECTED_NUM_ITEMS,
            ),
        }
    }
}

impl CountingRecordMap for ProbabilisticCountingRecordMap {
    fn insert(&mut self, record: &ProcessorRecord) {
        self.map.insert(record);
    }

    fn remove(&mut self, record: &ProcessorRecord) {
        self.map.remove(record);
    }

    fn estimate_count(&self, record: &ProcessorRecord) -> u64 {
        self.map.estimate_count(record) as u64
    }

    fn clear(&mut self) {
        self.map.clear();
    }

    fn serialize(
        &self,
        _record_store: &ProcessorRecordStore,
        object: &mut Object,
    ) -> Result<(), SerializationError> {
        serialize_bincode(&self.map, object)
    }
}

mod bloom;

#[cfg(test)]
mod tests {
    use dozer_core::processor_record::{ProcessorRecord, ProcessorRecordStore};
    use dozer_types::types::{Field, Record};

    use super::{
        AccurateCountingRecordMap, CountingRecordMap, CountingRecordMapEnum,
        ProbabilisticCountingRecordMap,
    };

    fn test_map(mut map: CountingRecordMapEnum) {
        let record_store = ProcessorRecordStore::new().unwrap();
        let make_record = |fields: Vec<Field>| -> ProcessorRecord {
            record_store.create_record(&Record::new(fields)).unwrap()
        };

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
        let accurate_map = AccurateCountingRecordMap::new().into();
        test_map(accurate_map);

        let probabilistic_map = ProbabilisticCountingRecordMap::new().into();
        test_map(probabilistic_map);
    }
}
