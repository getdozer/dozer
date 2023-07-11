use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_tracing::{
    IN_OPS_COUNTER_NAME, LATENCY_HISTOGRAME_NAME, LEFT_LOOKUP_SIZE_GAUGE_NAME,
    OUT_OPS_COUNTER_NAME, RIGHT_LOOKUP_SIZE_GAUGE_NAME, UNSATISFIED_JOINS_COUNTER_NAME,
};
use dozer_types::errors::internal::BoxedError;
use dozer_types::labels::Labels;
use dozer_types::types::Operation;
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    increment_counter,
};

use crate::pipeline::errors::PipelineError;

use super::operator::{JoinAction, JoinBranch, JoinOperator};

#[derive(Debug)]
pub struct ProductProcessor {
    join_operator: JoinOperator,
    labels: Labels,
}

impl ProductProcessor {
    pub fn new(id: String, join_operator: JoinOperator) -> Self {
        describe_gauge!(
            LEFT_LOOKUP_SIZE_GAUGE_NAME,
            "Total number of items in the left lookup table"
        );
        describe_gauge!(
            RIGHT_LOOKUP_SIZE_GAUGE_NAME,
            "Total number of items in the right lookup table"
        );
        describe_counter!(
            UNSATISFIED_JOINS_COUNTER_NAME,
            "Operations not matching the Join condition"
        );
        describe_counter!(
            IN_OPS_COUNTER_NAME,
            "Number of records received by the product processor"
        );
        describe_counter!(
            OUT_OPS_COUNTER_NAME,
            "Number of records forwarded by the product processor"
        );

        describe_histogram!(LATENCY_HISTOGRAME_NAME, "Processing latency");

        let mut labels = Labels::empty();
        labels.push("pid", id);
        Self {
            join_operator,
            labels,
        }
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

        let now = std::time::Instant::now();
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

                old_records.into_iter().chain(new_records).collect()
            }
        };

        let elapsed = now.elapsed();
        histogram!(LATENCY_HISTOGRAME_NAME, elapsed, self.labels.clone());
        increment_counter!(IN_OPS_COUNTER_NAME, self.labels.clone());

        counter!(
            OUT_OPS_COUNTER_NAME,
            records.len() as u64,
            self.labels.clone()
        );

        gauge!(
            LEFT_LOOKUP_SIZE_GAUGE_NAME,
            self.join_operator.left_lookup_size() as f64,
            self.labels.clone()
        );
        gauge!(
            RIGHT_LOOKUP_SIZE_GAUGE_NAME,
            self.join_operator.right_lookup_size() as f64,
            self.labels.clone()
        );

        if records.is_empty() {
            increment_counter!(UNSATISFIED_JOINS_COUNTER_NAME, self.labels.clone());
        }

        for (action, record) in records {
            match action {
                JoinAction::Insert => {
                    fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
                JoinAction::Delete => {
                    fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                }
            }
        }

        Ok(())
    }
}
