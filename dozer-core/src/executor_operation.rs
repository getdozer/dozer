use dozer_types::{epoch::Epoch, types::Operation};

use crate::processor_record::ProcessorRecord;

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

impl From<Operation> for ProcessorOperation {
    fn from(record: Operation) -> Self {
        match record {
            Operation::Delete { old } => ProcessorOperation::Delete { old: old.into() },
            Operation::Insert { new } => ProcessorOperation::Insert { new: new.into() },
            Operation::Update { old, new } => ProcessorOperation::Update {
                old: old.into(),
                new: new.into(),
            },
        }
    }
}

impl ProcessorOperation {
    pub fn clone_deref(&self) -> Operation {
        match self {
            ProcessorOperation::Delete { old } => Operation::Delete {
                old: old.clone_deref(),
            },
            ProcessorOperation::Insert { new } => Operation::Insert {
                new: new.clone_deref(),
            },
            ProcessorOperation::Update { old, new } => Operation::Update {
                old: old.clone_deref(),
                new: new.clone_deref(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorOperation {
    Op { op: ProcessorOperation },
    Commit { epoch: Epoch },
    Terminate,
    SnapshottingDone { connection_name: String },
}
