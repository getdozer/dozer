use std::collections::HashMap;

#[derive(Clone)]
pub enum Field {
    CharField(char),
    StringField(String),
    IntField(i64),
    FloatField(f64),
    BoolField(bool),
    BinaryField(Vec<u8>),
    TimestampField(u64),
    Empty,
}

#[derive(Clone)]
pub struct Schema {
    pub id: String,
    pub field_names: Vec<String>,
    pub field_types: Vec<Field>,
    _idx: HashMap<String, u16>,
    _ctr: u16,
}

#[derive(Clone)]
pub struct Record {
    pub values: Vec<Field>,
    pub schema_id: u64,
}

pub struct OperationEvent {
    pub operation: Operation,
    pub id: u32,
}

#[derive(Clone)]
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
pub struct TableInfo {
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
}
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
