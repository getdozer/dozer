use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum Field {
    Int(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Binary(Vec<u8>),
    #[serde(with = "rust_decimal::serde::float")]
    Decimal(Decimal),
    #[serde(with = "bson::serde_helpers::chrono_datetime_as_bson_datetime")]
    Timestamp(DateTime<Utc>),
    Bson(Vec<u8>),
    Null,
    Invalid(String),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Schema {
    pub id: String,
    pub field_names: Vec<String>,
    pub field_types: Vec<Field>,
    pub _idx: HashMap<String, u16>,
    pub _ctr: u16,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Record {
    pub values: Vec<Field>,
    pub schema_id: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OperationEvent {
    pub id: u32,
    pub operation: Operation,
}

impl OperationEvent {
    pub fn new(id: u32, operation: Operation) -> Self {
        Self { id, operation }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Operation {
    Delete {
        table_name: String,
        old: Record,
    },
    Insert {
        table_name: String,
        new: Record,
    },
    Update {
        table_name: String,
        old: Record,
        new: Record,
    },
    Terminate,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TableInfo {
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ColumnInfo {
    pub column_name: String,
    pub is_nullable: bool,
    pub udt_name: String,
    pub is_primary_key: bool,
}

impl Record {
    pub fn new(schema_id: u64, values: Vec<Field>) -> Record {
        Record { schema_id, values }
    }
}

impl Schema {
    pub fn new(id: String, field_names: Vec<String>, field_types: Vec<Field>) -> Schema {
        Schema {
            id: id,
            field_names: field_names,
            field_types: field_types,
            _idx: HashMap::new(),
            _ctr: 0,
        }
    }
}
