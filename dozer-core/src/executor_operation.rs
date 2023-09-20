use dozer_recordstore::{ProcessorRecord, ProcessorRecordStore};
use dozer_types::types::Operation;

use crate::{epoch::Epoch, errors::ExecutionError};

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

impl ProcessorOperation {
    pub fn new(
        op: &Operation,
        record_store: &ProcessorRecordStore,
    ) -> Result<Self, ExecutionError> {
        Ok(match op {
            Operation::Delete { old } => ProcessorOperation::Delete {
                old: record_store.create_record(old)?,
            },
            Operation::Insert { new } => ProcessorOperation::Insert {
                new: record_store.create_record(new)?,
            },
            Operation::Update { old, new } => ProcessorOperation::Update {
                old: record_store.create_record(old)?,
                new: record_store.create_record(new)?,
            },
        })
    }

    pub fn load(&self, record_store: &ProcessorRecordStore) -> Result<Operation, ExecutionError> {
        Ok(match self {
            ProcessorOperation::Delete { old } => Operation::Delete {
                old: record_store.load_record(old)?,
            },
            ProcessorOperation::Insert { new } => Operation::Insert {
                new: record_store.load_record(new)?,
            },
            ProcessorOperation::Update { old, new } => Operation::Update {
                old: record_store.load_record(old)?,
                new: record_store.load_record(new)?,
            },
        })
    }
}

#[derive(Clone, Debug)]
pub enum ExecutorOperation {
    Op { op: ProcessorOperation },
    Commit { epoch: Epoch },
    Terminate,
    SnapshottingDone { connection_name: String },
}
