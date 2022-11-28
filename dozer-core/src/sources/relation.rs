use crate::storage::record_reader::RecordReader;
use crate::storage::record_store::RecordStore;
use crate::storage::tx_store::TransactionStore;
use crossbeam::channel::Sender;
use dozer_types::types::{OperationEvent, Schema};
use std::time::Duration;

pub struct SchemaInfo {
    version: u32,
    schema: Schema,
}

pub struct RetentionOptions {
    duration: Duration,
    ts_field: Option<String>,
}

pub struct DatasetOptions {
    filter_fields: Option<Vec<String>>,
    unique_key: Option<Vec<String>>,
    retention: Option<RetentionOptions>,
}

pub struct RelationManager {
    id: String,
    record_store: RecordStore,
    tx_store: TransactionStore,
}

pub trait Relation {
    fn get_id(&self) -> String;
    fn get_record_reader(&self) -> RecordReader;
    fn get_schema(&self) -> SchemaInfo;
    fn subscribe(&self, sender: Sender<OperationEvent>) -> u32;
    fn unsubscribe(&self, id: u32);
}
