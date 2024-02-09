use dozer_types::{node::OpIdentifier, types::OperationWithId};

use crate::epoch::Epoch;

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
