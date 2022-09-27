use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

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
    RecordArray(Vec<Record>),
    Null,
    Invalid(String),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum FieldType {
    Int,
    Float,
    Boolean,
    String,
    Binary,
    Decimal,
    Timestamp,
    Bson,
    RecordArray(Schema)
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct FieldDefinition {
    pub name: String,
    pub typ: FieldType,
    pub nullable: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct SchemaIdentifier {
    pub id: u32,
    pub version: u16,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Schema {
    /// Unique identifier and version for this schema. This value is required only if teh schema
    /// is represented by a valid entry in teh schema registry. For nested schemas, this field
    /// is not applicable
    pub identifier: Option<SchemaIdentifier>,

    /// fields contains a list of FieldDefinition for all teh fields that appear in a record.
    /// Not necessarily all these fields will end up in teh final object structure stored in
    /// the cache. Some fields might only be used for indexing purposes only.
    pub fields: Vec<FieldDefinition>,

    /// Indexes of the fields representing values that will appear in teh final object stored
    /// in teh cache
    pub values: Vec<usize>,

    /// Indexes of the fields forming the primary key for this schema. If the value is empty
    /// only Insert Operation are supported. Updates and Deletes are not supported without a
    /// primary key definition
    pub primary_index: Vec<usize>,

    // Secondary indexes definitions
    pub secondary_indexes: Vec<IndexDefinition>,
}

impl Schema {
    pub fn get_id(&self) -> u32 {
        self.identifier.clone().unwrap().id
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum IndexType {
    SortedInverted,
    HashInverted,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct IndexDefinition {
    /// Indexes of the fields forming the index key
    pub fields: Vec<usize>,
    /// Type of index (i.e. hash inverted index, tree inverted index, full-text index, geo index, facet index, etc)
    pub typ: IndexType,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Record {
    /// Schema implemented by this Record
    pub schema_id: Option<SchemaIdentifier>,
    /// List of values, following the definitions of `fields` of the asscoiated schema
    pub values: Vec<Field>,
}

impl Record {
    pub fn new(schema_id: Option<SchemaIdentifier>, values: Vec<Field>) -> Record {
        Record { schema_id, values }
    }
    pub fn nulls(schema_id: Option<SchemaIdentifier>, size: usize) -> Record {
        Record {
            schema_id,
            values: vec![Field::Null; size],
        }
    }
    pub fn set_value(&mut self, idx: usize, value: Field) {
        self.values[idx] = value;
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OperationEvent {
    pub seq_no: u64,
    pub operation: Operation,
}

impl OperationEvent {
    pub fn new(seq_no: u64, operation: Operation) -> Self {
        Self { seq_no, operation }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum Operation {
    Delete { old: Record },
    Insert { new: Record },
    Update { old: Record, new: Record },
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
