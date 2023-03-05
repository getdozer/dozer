use crate::pipeline::errors::{PipelineError, ProductError};
use crate::pipeline::product::set::{SetAction, SetOperation};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::types::{Operation, Record};
use lmdb::{Database, DatabaseFlags};
use std::collections::HashMap;

#[derive(Debug)]
pub struct SetProcessor {
    /// Set operations
    operator: SetOperation,
    /// Database to store Join indexes
    db: Database,
}

impl SetProcessor {
    /// Creates a new [`FromProcessor`].
    pub fn new(
        operator: SetOperation,
        txn: &mut LmdbExclusiveTransaction,
    ) -> Result<Self, PipelineError> {
        Ok(Self {
            operator,
            db: txn.create_database(Some("set"), Some(DatabaseFlags::empty()))?,
        })
    }

    fn delete(
        &mut self,
        _from_port: PortHandle,
        record: &Record,
        txn: &SharedTransaction,
    ) -> Result<Vec<(SetAction, Record)>, ProductError> {
        self.operator
            .execute(SetAction::Delete, record, &self.db, txn)
            .map_err(|err| {
                ProductError::DeleteError("UNION query error:".to_string(), Box::new(err))
            })
    }

    fn insert(
        &mut self,
        _from_port: PortHandle,
        record: &Record,
        txn: &SharedTransaction,
    ) -> Result<Vec<(SetAction, Record)>, ProductError> {
        self.operator
            .execute(SetAction::Insert, record, &self.db, txn)
            .map_err(|err| {
                ProductError::InsertError("UNION query error:".to_string(), Box::new(err))
            })
    }

    #[allow(clippy::type_complexity)]
    fn update(
        &mut self,
        _from_port: PortHandle,
        old: &Record,
        new: &Record,
        txn: &SharedTransaction,
    ) -> Result<(Vec<(SetAction, Record)>, Vec<(SetAction, Record)>), ProductError> {
        let old_records = self
            .operator
            .execute(SetAction::Delete, old, &self.db, txn)
            .map_err(|err| {
                ProductError::UpdateOldError("UNION query error:".to_string(), Box::new(err))
            })?;

        let new_records = self
            .operator
            .execute(SetAction::Insert, new, &self.db, txn)
            .map_err(|err| {
                ProductError::UpdateNewError("UNION query error:".to_string(), Box::new(err))
            })?;

        Ok((old_records, new_records))
    }
}

impl Processor for SetProcessor {
    fn commit(&self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        transaction: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        fw.send(op, DEFAULT_PORT_HANDLE);

        // match op {
        //     Operation::Delete { ref old } => {
        //         let records = self
        //             .delete(from_port, old, transaction, reader)
        //             .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;
        //
        //         for (action, record) in records.into_iter() {
        //             match action {
        //                 SetAction::Insert => {
        //                     let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
        //                 }
        //                 SetAction::Delete => {
        //                     let _ = fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
        //                 }
        //             }
        //         }
        //     }
        //     Operation::Insert { ref new } => {
        //         let records = self
        //             .insert(from_port, new, transaction, reader)
        //             .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;
        //
        //         for (action, record) in records.into_iter() {
        //             match action {
        //                 SetAction::Insert => {
        //                     let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
        //                 }
        //                 SetAction::Delete => {
        //                     let _ = fw.send(Operation::Delete { old: record }, DEFAULT_PORT_HANDLE);
        //                 }
        //             }
        //         }
        //     }
        //     Operation::Update { ref old, ref new } => {
        //         let (old_records, new_records) = self
        //             .update(from_port, old, new, transaction, reader)
        //             .map_err(|err| ExecutionError::ProductProcessorError(Box::new(err)))?;
        //
        //         for (action, old) in old_records.into_iter() {
        //             match action {
        //                 SetAction::Insert => {
        //                     let _ = fw.send(Operation::Insert { new: old }, DEFAULT_PORT_HANDLE);
        //                 }
        //                 SetAction::Delete => {
        //                     let _ = fw.send(Operation::Delete { old }, DEFAULT_PORT_HANDLE);
        //                 }
        //             }
        //         }
        //
        //         for (action, new) in new_records.into_iter() {
        //             match action {
        //                 SetAction::Insert => {
        //                     let _ = fw.send(Operation::Insert { new }, DEFAULT_PORT_HANDLE);
        //                 }
        //                 SetAction::Delete => {
        //                     let _ = fw.send(Operation::Delete { old: new }, DEFAULT_PORT_HANDLE);
        //                 }
        //             }
        //         }
        //     }
        // }
        Ok(())
    }
}
