use crate::{epoch::Epoch, processor_record::ProcessorRecord};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorOperation {
    Op { op: ProcessorOperation },
    Commit { epoch: Epoch },
    Terminate,
    SnapshottingDone { connection_name: String },
}
