use std::hash::Hash;
use std::sync::Arc;

use dozer_types::{
    serde::{Deserialize, Serialize},
    types::{Field, Lifetime},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct RecordRef(Arc<[Field]>);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ProcessorRecord {
    /// All `Field`s in this record. The `Field`s are grouped by `Arc` to reduce memory usage.
    /// This is a Box<[]> instead of a Vec to save space on storing the vec's capacity
    values: Box<[RecordRef]>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    lifetime: Option<Box<Lifetime>>,
}

impl ProcessorRecord {
    pub fn new(values: Box<[RecordRef]>) -> Self {
        Self {
            values,
            ..Default::default()
        }
    }

    pub fn get_lifetime(&self) -> Option<Lifetime> {
        self.lifetime.as_ref().map(|lifetime| *lifetime.clone())
    }
    pub fn set_lifetime(&mut self, lifetime: Option<Lifetime>) {
        self.lifetime = lifetime.map(Box::new);
    }

    pub fn values(&self) -> &[RecordRef] {
        &self.values
    }

    pub fn appended(existing: &ProcessorRecord, additional: RecordRef) -> Self {
        let mut values = Vec::with_capacity(existing.values().len() + 1);
        values.extend_from_slice(existing.values());
        values.push(additional);
        Self::new(values.into_boxed_slice())
    }
}

mod store;
pub use store::ProcessorRecordStore;
