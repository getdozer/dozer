use crate::types::{Record, Schema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum Event {
    SchemaChange(Schema),
    RecordUpdate(Record),
    RecordInsert(Record),
    RecordDelete(Record),
}
