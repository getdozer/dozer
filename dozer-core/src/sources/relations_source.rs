use crate::dag::errors::ExecutionError;
use crate::storage::record_reader::RecordReader;
use crossbeam::channel::Sender;
use dozer_types::types::{OperationEvent, Record, Schema};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

// pub trait RelationsSource {
//     fn get_relations_mappings(&self) -> HashMap<String, u32>;
//     fn start(&self) -> Result<(), ExecutionError>;
//     fn stop(&self) -> Result<(), ExecutionError>;
// }

pub struct RelationsSourceStartSettings {
    stop_req: Arc<AtomicBool>,
}

pub trait RelationsSourceForwarder {
    fn insert_record(
        &self,
        rel_id: u32,
        tx_id: u64,
        last_in_tx: bool,
        new: Record,
    ) -> Result<(), ExecutionError>;
    fn delete_record(
        &self,
        rel_id: u32,
        tx_id: u64,
        last_in_tx: bool,
        old: Record,
    ) -> Result<(), ExecutionError>;
    fn update_record(
        &self,
        rel_id: u32,
        tx_id: u64,
        last_in_tx: bool,
        old: Record,
        new: Record,
    ) -> Result<(), ExecutionError>;
    fn update_schema(&self, rel_id: u32, schmea: Schema) -> Result<(), ExecutionError>;
}

pub struct RelationsSource {
    mappings: HashMap<String, u32>,
    runner: Box<dyn Fn(RelationsSourceStartSettings) -> Result<(), ExecutionError>>,
}

impl RelationsSource {
    pub fn new(
        mappings: HashMap<String, u32>,
        runner: Box<dyn Fn(RelationsSourceStartSettings) -> Result<(), ExecutionError>>,
    ) -> Self {
        Self { mappings, runner }
    }
}
