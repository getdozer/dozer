use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::Operation;

use crate::pipeline::errors::PipelineError;

use super::operator::{JoinAction, JoinBranch, JoinOperator};

#[derive(Debug)]
pub struct ProductProcessor {
    join_operator: JoinOperator,
}

impl ProductProcessor {
    pub fn new(join_operator: JoinOperator) -> Self {
        Self { join_operator }
    }

    fn update_eviction_index(&mut self, lifetime: &dozer_types::types::Lifetime) {
        let now = &lifetime.reference;

        let old_instants = self.join_operator.evict_index(&JoinBranch::Left, now);
        self.join_operator
            .clean_evict_index(&JoinBranch::Left, &old_instants);
        let old_instants = self.join_operator.evict_index(&JoinBranch::Right, now);
        self.join_operator
            .clean_evict_index(&JoinBranch::Right, &old_instants);
    }
}

impl Processor for ProductProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let from_branch = match from_port {
            0 => &JoinBranch::Left,
            1 => &JoinBranch::Right,
            _ => return Err(PipelineError::InvalidPort(from_port).into()),
        };

        let records = match op {
            Operation::Delete { ref old } => {
                if let Some(lifetime) = &old.lifetime {
                    self.update_eviction_index(lifetime);
                }

                self.join_operator
                    .delete(from_branch, old)
                    .map_err(PipelineError::JoinError)?
            }
            Operation::Insert { ref new } => {
                if let Some(lifetime) = &new.lifetime {
                    self.update_eviction_index(lifetime);
                }

                self.join_operator
                    .insert(from_branch, new)
                    .map_err(PipelineError::JoinError)?
            }
            Operation::Update { ref old, ref new } => {
                if let Some(lifetime) = &old.lifetime {
                    self.update_eviction_index(lifetime);
                }

                let old_records = self
                    .join_operator
                    .delete(from_branch, old)
                    .map_err(PipelineError::JoinError)?;

                let new_records = self
                    .join_operator
                    .insert(from_branch, new)
                    .map_err(PipelineError::JoinError)?;

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
