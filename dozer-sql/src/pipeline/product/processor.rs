use crate::pipeline::errors::{PipelineError, ProductError};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::record_store::RecordReader;
use dozer_core::DEFAULT_PORT_HANDLE;

use dozer_core::storage::lmdb_storage::SharedTransaction;
use dozer_types::types::{Operation, Record};
use std::collections::HashMap;

use super::join::{JoinAction, JoinSource};

/// Cartesian Product Processor
#[derive(Debug)]
pub struct FromProcessor {
    /// Join operations
    operator: JoinSource,

    source_names: HashMap<PortHandle, String>,
}

impl FromProcessor {
    /// Creates a new [`FromProcessor`].
    pub fn new(
        operator: JoinSource,
        source_names: HashMap<PortHandle, String>,
    ) -> Result<Self, PipelineError> {
        Ok(Self {
            operator,
            source_names,
        })
    }

    fn delete(
        &mut self,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        self.operator
            .execute(JoinAction::Delete, from_port, record)
            .map_err(|err| ProductError::DeleteError(self.get_port_name(from_port), Box::new(err)))
    }

    fn insert(
        &mut self,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Vec<u8>)>, ProductError> {
        self.operator
            .execute(JoinAction::Insert, from_port, record)
            .map_err(|err| ProductError::InsertError(self.get_port_name(from_port), Box::new(err)))
    }

    #[allow(clippy::type_complexity)]
    fn update(
        &mut self,
        from_port: PortHandle,
        old: &Record,
        new: &Record,
    ) -> Result<
        (
            Vec<(JoinAction, Record, Vec<u8>)>,
            Vec<(JoinAction, Record, Vec<u8>)>,
        ),
        ProductError,
    > {
        let old_records = self
            .operator
            .execute(JoinAction::Delete, from_port, old)
            .map_err(|err| {
                ProductError::UpdateOldError(self.get_port_name(from_port), Box::new(err))
            })?;

        let new_records = self
            .operator
            .execute(JoinAction::Insert, from_port, new)
            .map_err(|err| {
                ProductError::UpdateNewError(self.get_port_name(from_port), Box::new(err))
            })?;

        Ok((old_records, new_records))
    }

    fn get_port_name(&self, from_port: u16) -> String {
        self.source_names
            .get(&from_port)
            .unwrap_or(&from_port.to_string())
            .to_string()
    }
}

impl Processor for FromProcessor {
    fn commit(&self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        _transaction: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        // match op.clone() {
        //     Operation::Delete { old } => info!("p{from_port}: - {:?}", old.values),
        //     Operation::Insert { new } => info!("p{from_port}: + {:?}", new.values),
        //     Operation::Update { old, new } => {
        //         info!("p{from_port}: - {:?}, + {:?}", old.values, new.values)
        //     }
        // }

        match op {
            Operation::Delete { ref old } => {
                let records = self
                    .delete(from_port, old)
                    .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;

                for (action, record, _key) in records.into_iter() {
                    match action {
                        JoinAction::Insert => {
                            let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                        }
                        JoinAction::Delete => {
                            let _ = fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }
            }
            Operation::Insert { ref new } => {
                let records = self
                    .insert(from_port, new)
                    .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;

                for (action, record, _key) in records.into_iter() {
                    match action {
                        JoinAction::Insert => {
                            let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                        }
                        JoinAction::Delete => {
                            let _ = fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }
            }
            Operation::Update { ref old, ref new } => {
                let (old_join_records, new_join_records) = self
                    .update(from_port, old, new)
                    .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;

                for (action, old, _key) in old_join_records.into_iter() {
                    match action {
                        JoinAction::Insert => {
                            let _ = fw.send(Operation::Insert { new: old }, DEFAULT_PORT_HANDLE);
                        }
                        JoinAction::Delete => {
                            let _ = fw.send(Operation::Delete { old }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }

                for (action, new, _key) in new_join_records.into_iter() {
                    match action {
                        JoinAction::Insert => {
                            let _ = fw.send(Operation::Insert { new }, DEFAULT_PORT_HANDLE);
                        }
                        JoinAction::Delete => {
                            let _ = fw.send(Operation::Delete { old: new }, DEFAULT_PORT_HANDLE);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
