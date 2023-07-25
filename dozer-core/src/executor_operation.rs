use dozer_storage::errors::StorageError;
use dozer_types::{epoch::Epoch, types::Operation};

use crate::processor_record::{ProcessorRecordRef, ProcessorRecordStore};

#[derive(Clone, Debug, PartialEq, Eq)]
/// A CDC event.
pub enum ProcessorOperation {
    Delete {
        old: ProcessorRecordRef,
    },
    Insert {
        new: ProcessorRecordRef,
    },
    Update {
        old: ProcessorRecordRef,
        new: ProcessorRecordRef,
    },
}

impl ProcessorOperation {
    pub fn new(record_store: &ProcessorRecordStore, op: Operation) -> Result<Self, StorageError> {
        Ok(match op {
            Operation::Delete { old } => ProcessorOperation::Delete {
                old: record_store.create_ref(old.into())?,
            },
            Operation::Insert { new } => ProcessorOperation::Insert {
                new: record_store.create_ref(new.into())?,
            },
            Operation::Update { old, new } => ProcessorOperation::Update {
                old: record_store.create_ref(old.into())?,
                new: record_store.create_ref(new.into())?,
            },
        })
    }

    pub fn clone_deref(
        &self,
        record_store: &ProcessorRecordStore,
    ) -> Result<Operation, StorageError> {
        Ok(match self {
            ProcessorOperation::Delete { old } => Operation::Delete {
                old: record_store.get_record(old)?.clone_deref(record_store)?,
            },
            ProcessorOperation::Insert { new } => Operation::Insert {
                new: record_store.get_record(new)?.clone_deref(record_store)?,
            },
            ProcessorOperation::Update { old, new } => Operation::Update {
                old: record_store.get_record(old)?.clone_deref(record_store)?,
                new: record_store.get_record(new)?.clone_deref(record_store)?,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorOperation {
    Op { op: ProcessorOperation },
    Commit { epoch: Epoch },
    Terminate,
    SnapshottingDone { connection_name: String },
}
