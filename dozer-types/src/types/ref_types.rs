use std::hash::Hash;
use std::sync::Arc;

use super::{Field, Lifetime, Record};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessorRecord {
    /// List of values, following the definitions of `fields` of the associated schema
    pub values: Vec<RefOrField>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    pub lifetime: Option<Lifetime>,

    // Indexes of the fields to be used if this is a reference record
    pub index: Vec<u32>,
}

impl ProcessorRecord {
    pub fn new() -> Self {
        ProcessorRecord {
            values: Vec::new(),
            lifetime: None,
            index: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]

pub enum RefOrField {
    Ref(RecordRef),
    Field(Field),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecordRef(Arc<Record>);

impl From<Record> for ProcessorRecord {
    fn from(record: Record) -> Self {
        let mut ref_record = ProcessorRecord::new();
        for field in record.values {
            ref_record.values.push(RefOrField::Field(field));
        }
        ref_record
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
/// A CDC event.
pub enum ProcessorOperation {
    Delete {
        old: ProcessorRecord,
    },
    Insert {
        new: ProcessorRecord,
    },
    Update {
        old: ProcessorRecord,
        new: ProcessorRecord,
    },
}
