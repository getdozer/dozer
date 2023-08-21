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
    values: Vec<RecordRef>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    lifetime: Option<Box<Lifetime>>,
}

impl ProcessorRecord {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_lifetime(&self) -> Option<Lifetime> {
        self.lifetime.as_ref().map(|lifetime| *lifetime.clone())
    }
    pub fn set_lifetime(&mut self, lifetime: Option<Lifetime>) {
        self.lifetime = lifetime.map(Box::new);
    }

    pub fn extend(&mut self, other: ProcessorRecord) {
        self.values.extend(other.values);
    }

    pub fn push(&mut self, record_ref: RecordRef) {
        self.values.push(record_ref);
    }

    pub fn pop(&mut self) -> Option<RecordRef> {
        self.values.pop()
    }
}

mod store;
pub use store::ProcessorRecordStore;
