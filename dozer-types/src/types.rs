use ahash::AHasher;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;

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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum FieldType {
    Int,
    Float,
    Boolean,
    String,
    Binary,
    Decimal,
    Timestamp,
    Bson,
    Null,
    RecordArray(Schema),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct FieldDefinition {
    pub name: String,
    pub typ: FieldType,
    pub nullable: bool,
}

impl FieldDefinition {
    pub fn new(name: String, typ: FieldType, nullable: bool) -> Self {
        Self {
            name,
            typ,
            nullable,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SchemaIdentifier {
    pub id: u32,
    pub version: u16,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
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
    pub fn empty() -> Schema {
        Self {
            identifier: None,
            fields: Vec::new(),
            values: Vec::new(),
            primary_index: Vec::new(),
            secondary_indexes: Vec::new(),
        }
    }

    pub fn field(&mut self, f: FieldDefinition, value: bool, pk: bool) -> &mut Self {
        self.fields.push(f);
        if value {
            self.values.push(&self.fields.len() - 1)
        }
        if pk {
            self.primary_index.push(&self.fields.len() - 1)
        }
        self
    }

    pub fn get_id(&self) -> u32 {
        self.identifier.clone().unwrap().id
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum IndexType {
    SortedInverted,
    HashInverted,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct IndexDefinition {
    /// Indexes of the fields forming the index key
    pub fields: Vec<usize>,
    pub sort_direction: Vec<bool>,
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

    pub fn get_key(&self, indexes: Vec<usize>) -> anyhow::Result<Vec<u8>> {
        let mut r = Vec::<u8>::new();
        for i in indexes {
            match &self.values[i] {
                Field::Int(i) => {
                    r.extend(i.to_ne_bytes());
                }
                Field::Float(f) => {
                    r.extend(f.to_ne_bytes());
                }
                Field::Boolean(b) => r.extend(if *b {
                    1_u8.to_ne_bytes()
                } else {
                    0_u8.to_ne_bytes()
                }),
                Field::String(s) => {
                    r.extend(s.as_bytes());
                }
                Field::Binary(b) => {
                    r.extend(b);
                }
                Field::Decimal(d) => {
                    r.extend(d.serialize());
                }
                Field::Timestamp(t) => r.extend(t.timestamp().to_ne_bytes()),
                Field::Bson(b) => {
                    r.extend(b);
                }
                Field::Null => r.extend(0_u8.to_ne_bytes()),
                _ => {
                    return Err(anyhow!("Invalid field type"));
                }
            }
        }
        Ok(r)
    }

    pub fn get_hash(&self, indexes: Vec<usize>) -> anyhow::Result<u64> {
        let mut hasher = AHasher::default();

        for i in indexes.iter().enumerate() {
            hasher.write_usize(i.0);
            match &self.values[*i.1] {
                Field::Int(i) => {
                    hasher.write_u8(1);
                    hasher.write_i64(*i);
                }
                Field::Float(f) => {
                    hasher.write_u8(2);
                    hasher.write(&((*f).to_ne_bytes()));
                }
                Field::Boolean(b) => {
                    hasher.write_u8(3);
                    hasher.write_u8(if *b { 1_u8 } else { 0_u8 });
                }
                Field::String(s) => {
                    hasher.write_u8(4);
                    hasher.write(s.as_str().as_bytes());
                }
                Field::Binary(b) => {
                    hasher.write_u8(5);
                    hasher.write(b.as_ref());
                }
                Field::Decimal(d) => {
                    hasher.write_u8(6);
                    hasher.write(&d.serialize());
                }
                Field::Timestamp(t) => {
                    hasher.write_u8(7);
                    hasher.write_i64(t.timestamp())
                }
                Field::Bson(b) => {
                    hasher.write_u8(8);
                    hasher.write(b.as_ref());
                }
                Field::Null => {
                    hasher.write_u8(0);
                }
                _ => {
                    return Err(anyhow!("Invalid field type"));
                }
            }
        }
        Ok(hasher.finish())
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
    SchemaUpdate { new: Schema },
    Terminate,
}
