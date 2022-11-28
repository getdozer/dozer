use crate::storage::record_reader::RecordReader;
use crossbeam::channel::Sender;
use dozer_types::types::{OperationEvent, Schema};

pub struct SchemaVersion {
    version: u32,
    schema: Schema,
}

pub trait Dataset {
    fn id() -> String;
    fn get_record_reader() -> RecordReader;
    fn get_schema() -> Schema;
    fn subscribe(sender: Sender<OperationEvent>) -> u32;
    fn unsubscribe(id: u32);
}
