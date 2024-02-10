use dozer_types::{node::OpIdentifier, types::TableOperation};

use crate::epoch::Epoch;

#[derive(Clone, Debug)]
pub enum ExecutorOperation {
    Op {
        op: TableOperation,
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
