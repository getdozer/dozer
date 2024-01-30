use dozer_recordstore::{ProcessorRecord, StoreRecord};
use dozer_types::{
    node::OpIdentifier,
    types::{Operation, OperationWithId},
};

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
    BatchInsert {
        new: Vec<ProcessorRecord>,
    },
}

impl ProcessorOperation {
    pub fn new(op: &Operation, record_store: &impl StoreRecord) -> Result<Self, ExecutionError> {
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
            Operation::BatchInsert { new } => ProcessorOperation::BatchInsert {
                new: new
                    .iter()
                    .map(|record| record_store.create_record(record))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        })
    }

    pub fn load(&self, record_store: &impl StoreRecord) -> Result<Operation, ExecutionError> {
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
            ProcessorOperation::BatchInsert { new } => Operation::BatchInsert {
                new: new
                    .iter()
                    .map(|record| record_store.load_record(record))
                    .collect::<Result<Vec<_>, _>>()?,
            },
        })
    }
}

#[derive(Clone, Debug)]
pub enum ExecutorOperation {
    Op {
        op: OperationWithId,
    },
    Commit {
        epoch: Epoch,
    },
    Terminate,
    SnapshottingStarted {
        connection_name: String,
    },
    SnapshottingDone {
        connection_name: String,
        id: Option<OpIdentifier>,
    },
}
