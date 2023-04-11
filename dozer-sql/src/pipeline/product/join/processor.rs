use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::Operation;

use super::operator::{JoinAction, JoinBranch, JoinOperator};

#[derive(Debug)]
pub struct ProductProcessor {
    join_operator: JoinOperator,
}

impl ProductProcessor {
    pub fn new(join_operator: JoinOperator) -> Self {
        Self { join_operator }
    }
}

impl Processor for ProductProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), ExecutionError> {
        let from_branch = match from_port {
            0 => &JoinBranch::Left,
            1 => &JoinBranch::Right,
            _ => return Err(ExecutionError::InvalidPort(from_port)),
        };
        let records = match op {
            Operation::Delete { ref old } => self
                .join_operator
                .delete(from_branch, old)
                .map_err(|err| ExecutionError::InternalError(Box::new(err)))?,
            Operation::Insert { ref new } => self
                .join_operator
                .insert(from_branch, new)
                .map_err(|err| ExecutionError::InternalError(Box::new(err)))?,
            Operation::Update { ref old, ref new } => {
                let old_records = self
                    .join_operator
                    .delete(from_branch, old)
                    .map_err(|err| ExecutionError::InternalError(Box::new(err)))?;

                let new_records = self
                    .join_operator
                    .insert(from_branch, new)
                    .map_err(|err| ExecutionError::InternalError(Box::new(err)))?;

                old_records
                    .into_iter()
                    .chain(new_records.into_iter())
                    .collect()
            }
        };

        for (action, record) in records {
            match action {
                JoinAction::Insert => {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE)?;
                }
                JoinAction::Delete => {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE)?;
                }
            }
        }

        Ok(())
    }
}
