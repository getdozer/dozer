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
    pub _idx: HashMap<String, usize>,
    pub _ctr: u16,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Record {
    pub values: Vec<Field>,
    pub schema_id: u64,
}

impl Record {
    pub fn new(schema_id: u64, values: Vec<Field>) -> Record {
        Record { schema_id, values }
    }
    pub fn nulls(schema_id: u64, size: usize) -> Record { Record { schema_id, values: vec![Field::Null; size] } }
    pub fn set_value(&mut self, idx: usize, value: Field) { self.values[idx] = value; }
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
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


impl Schema {
    pub fn new(id: String, field_names: Vec<String>, field_types: Vec<Field>) -> Schema {
        let mut indexes = HashMap::new();
        let mut c = 0;
        for i in field_names.iter() {
            indexes.insert(i.clone(), c);
            c = c + 1;
        }
        Schema {
            id,
            field_names,
            field_types,
            _idx: indexes,
            _ctr: 0,
        }
    }

    pub fn get_column_index(&self, name: String) -> Option<&usize> {
        self._idx.get(&name)
    }
}
