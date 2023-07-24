use dozer_types::{epoch::Epoch, types::Operation};

use crate::processor_record::ProcessorRecordRef;

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

impl From<Operation> for ProcessorOperation {
    fn from(record: Operation) -> Self {
        match record {
            Operation::Delete { old } => ProcessorOperation::Delete {
                old: ProcessorRecordRef::new(old.into()),
            },
            Operation::Insert { new } => ProcessorOperation::Insert {
                new: ProcessorRecordRef::new(new.into()),
            },
            Operation::Update { old, new } => ProcessorOperation::Update {
                old: ProcessorRecordRef::new(old.into()),
                new: ProcessorRecordRef::new(new.into()),
            },
        }
    }
}

impl ProcessorOperation {
    pub fn clone_deref(&self) -> Operation {
        match self {
            ProcessorOperation::Delete { old } => Operation::Delete {
                old: old.get_record().clone_deref(),
            },
            ProcessorOperation::Insert { new } => Operation::Insert {
                new: new.get_record().clone_deref(),
            },
            ProcessorOperation::Update { old, new } => Operation::Update {
                old: old.get_record().clone_deref(),
                new: new.get_record().clone_deref(),
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
