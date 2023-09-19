use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::processor_record::ProcessorRecordStore;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_tracing::Labels;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::Lifetime;
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    increment_counter,
};

use crate::errors::PipelineError;

use super::operator::{JoinAction, JoinBranch, JoinOperator};

#[derive(Debug)]
pub struct ProductProcessor {
    join_operator: JoinOperator,
    labels: Labels,
}

const LEFT_LOOKUP_SIZE: &str = "product.left_lookup_size";
const RIGHT_LOOKUP_SIZE: &str = "product.right_lookup_size";
const UNSATISFIED_JOINS: &str = "product.unsatisfied_joins";
const IN_OPS: &str = "product.in_ops";
const OUT_OPS: &str = "product.out_ops";
const LATENCY: &str = "product.latency";

impl ProductProcessor {
    pub fn new(id: String, join_operator: JoinOperator) -> Self {
        describe_gauge!(
            LEFT_LOOKUP_SIZE,
            "Total number of items in the left lookup table"
        );
        describe_gauge!(
            RIGHT_LOOKUP_SIZE,
            "Total number of items in the right lookup table"
        );
        describe_counter!(
            UNSATISFIED_JOINS,
            "Operations not matching the Join condition"
        );
        describe_counter!(
            IN_OPS,
            "Number of records received by the product processor"
        );
        describe_counter!(
            OUT_OPS,
            "Number of records forwarded by the product processor"
        );

        describe_histogram!(LATENCY, "Processing latency");

        let mut labels = Labels::empty();
        labels.push("pid", id);
        Self {
            join_operator,
            labels,
        }
    }

    fn update_eviction_index(&mut self, lifetime: Lifetime) {
        self.join_operator.evict_index(&lifetime.reference);
    }
}

impl Processor for ProductProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let from_branch = match from_port {
            0 => JoinBranch::Left,
            1 => JoinBranch::Right,
            _ => return Err(PipelineError::InvalidPortHandle(from_port).into()),
        };

        let now = std::time::Instant::now();
        let records = match op {
            ProcessorOperation::Delete { old } => {
                if let Some(lifetime) = old.get_lifetime() {
                    self.update_eviction_index(lifetime);
                }

                let old_decoded = record_store.load_record(&old)?;
                self.join_operator.delete(from_branch, &old, &old_decoded)
            }
            ProcessorOperation::Insert { new } => {
                if let Some(lifetime) = new.get_lifetime() {
                    self.update_eviction_index(lifetime);
                }

                let new_decoded = record_store.load_record(&new)?;
                self.join_operator
                    .insert(from_branch, &new, &new_decoded)
                    .map_err(PipelineError::JoinError)?
            }
            ProcessorOperation::Update { old, new } => {
                if let Some(lifetime) = old.get_lifetime() {
                    self.update_eviction_index(lifetime);
                }

                let old_decoded = record_store.load_record(&old)?;
                let new_decoded = record_store.load_record(&new)?;

                let mut old_records = self.join_operator.delete(from_branch, &old, &old_decoded);

                let new_records = self
                    .join_operator
                    .insert(from_branch, &new, &new_decoded)
                    .map_err(PipelineError::JoinError)?;

                old_records.extend(new_records);
                old_records
            }
        };

        let elapsed = now.elapsed();
        histogram!(LATENCY, elapsed, self.labels.clone());
        increment_counter!(IN_OPS, self.labels.clone());

        counter!(OUT_OPS, records.len() as u64, self.labels.clone());

        gauge!(
            LEFT_LOOKUP_SIZE,
            self.join_operator.left_lookup_size() as f64,
            self.labels.clone()
        );
        gauge!(
            RIGHT_LOOKUP_SIZE,
            self.join_operator.right_lookup_size() as f64,
            self.labels.clone()
        );

        if records.is_empty() {
            increment_counter!(UNSATISFIED_JOINS, self.labels.clone());
        }

        for (action, record) in records {
            match action {
                JoinAction::Insert => {
                    fw.send(
                        ProcessorOperation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
                JoinAction::Delete => {
                    fw.send(
                        ProcessorOperation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    );
                }
            }
        }

        Ok(())
    }

    fn serialize(
        &mut self,
        record_store: &ProcessorRecordStore,
        object: Object,
    ) -> Result<(), BoxedError> {
        self.join_operator
            .serialize(record_store, object)
            .map_err(Into::into)
    }
}
