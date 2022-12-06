use crate::errors::types::TypeError;
use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Utc};
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde::{self, Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub const DATE_FORMAT: &str = "%Y-%m-%d";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Field {
    UInt(u64),
    Int(i64),
    Float(OrderedFloat<f64>),
    Boolean(bool),
    String(String),
    Text(String),
    Binary(Vec<u8>),
    #[serde(with = "rust_decimal::serde::float")]
    Decimal(Decimal),
    Timestamp(DateTime<FixedOffset>),
    Date(NaiveDate),
    Bson(Vec<u8>),
    Null,
}

impl Display for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[macro_export]
macro_rules! serialize {
    ($self:ident, $stmt:expr) => {
        Ok([
            Self::get_type_prefix($self.get_type().unwrap()),
            Vec::from($stmt),
        ]
        .concat())
    };
}

#[macro_export]
macro_rules! deserialize {
    ($stmt:expr) => {
       $stmt.try_into().unwrap()
    };
}

impl Field {
    pub fn to_bytes(&self) -> Result<Vec<u8>, TypeError> {
        // prefix representing return type is added using get_type_prefix
        match self {
            Field::Int(i) => serialize!(self, i.to_be_bytes()),
            Field::UInt(i) => serialize!(self, i.to_be_bytes()),
            Field::Float(f) => serialize!(self, f.to_be_bytes()),
            Field::Boolean(b) => serialize!(
                self,
                if *b {
                    1_u8.to_be_bytes()
                } else {
                    0_u8.to_be_bytes()
                }
            ),
            Field::String(s) => serialize!(self, s.as_bytes()),
            Field::Text(t) => serialize!(self, t.as_bytes()),
            Field::Binary(b) => serialize!(self, b.as_slice()),
            Field::Decimal(d) => serialize!(self, d.serialize()),
            Field::Timestamp(t) => serialize!(self, t.timestamp_millis().to_be_bytes()),
            Field::Date(d) => serialize!(self, d.to_string().as_bytes()),
            Field::Bson(b) => serialize!(self, b.clone()),
            Field::Null => serialize!(self, 0_u8.to_be_bytes()),
        }
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Field, TypeError> {
        let offset: usize = 1;
        let return_type = Self::from_type_prefix(Box::from(buf[0..offset].to_vec()));
        let val = buf[1..buf.len()].as_ref();
        match return_type {
            FieldType::Int => Ok(Field::Int(i64::from_be_bytes(deserialize!(val)))),
            FieldType::UInt => Ok(Field::UInt(u64::from_be_bytes(deserialize!(val)))),
            FieldType::Float => Ok(Field::Float(OrderedFloat::from(f64::from_be_bytes(
                deserialize!(val),
            )))),
            FieldType::Boolean => Ok(Field::Boolean(*val == [1_u8])),
            FieldType::String => Ok(Field::String(
                String::from_utf8(deserialize!(val)).unwrap(),
            )),
            FieldType::Text => Ok(Field::Text(
                String::from_utf8(deserialize!(val)).unwrap(),
            )),
            FieldType::Binary => Ok(Field::Binary(val.try_into().unwrap())),
            FieldType::Decimal => Ok(Field::Decimal(Decimal::deserialize(
                deserialize!(val),
            ))),
            FieldType::Timestamp => Ok(Field::Timestamp(DateTime::from(
                Utc.timestamp_millis(i64::from_be_bytes(deserialize!(val))),
            ))),
            FieldType::Date => Ok(Field::Date(
                NaiveDate::parse_from_str(
                    String::from_utf8(deserialize!(val)).unwrap().as_ref(),
                    DATE_FORMAT,
                )
                .unwrap(),
            )),
            FieldType::Bson => Ok(Field::Bson(deserialize!(val))),
            FieldType::Null => Ok(Field::Null),
        }
    }

    pub fn get_type(&self) -> Result<FieldType, TypeError> {
        match self {
            Field::Int(_i) => Ok(FieldType::Int),
            Field::UInt(_i) => Ok(FieldType::UInt),
            Field::Float(_f) => Ok(FieldType::Float),
            Field::Boolean(_b) => Ok(FieldType::Boolean),
            Field::String(_s) => Ok(FieldType::String),
            Field::Text(_s) => Ok(FieldType::Text),
            Field::Binary(_b) => Ok(FieldType::Binary),
            Field::Decimal(_d) => Ok(FieldType::Decimal),
            Field::Timestamp(_t) => Ok(FieldType::Timestamp),
            Field::Date(_t) => Ok(FieldType::Date),
            Field::Bson(_b) => Ok(FieldType::Bson),
            Field::Null => Ok(FieldType::Null),
        }
    }

    pub fn get_type_prefix(f: FieldType) -> Vec<u8> {
        match f {
            FieldType::Int => vec![0_u8],
            FieldType::UInt => vec![1_u8],
            FieldType::Float => vec![2_u8],
            FieldType::Boolean => vec![3_u8],
            FieldType::String => vec![4_u8],
            FieldType::Text => vec![5_u8],
            FieldType::Binary => vec![6_u8],
            FieldType::Decimal => vec![7_u8],
            FieldType::Timestamp => vec![8_u8],
            FieldType::Date => vec![9_u8],
            FieldType::Bson => vec![10_u8],
            FieldType::Null => vec![11_u8],
        }
    }

    pub fn from_type_prefix(prefix: Box<[u8]>) -> FieldType {
        match prefix.as_ref() {
            [0_u8] => FieldType::Int,
            [1_u8] => FieldType::UInt,
            [2_u8] => FieldType::Float,
            [3_u8] => FieldType::Boolean,
            [4_u8] => FieldType::String,
            [5_u8] => FieldType::Text,
            [6_u8] => FieldType::Binary,
            [7_u8] => FieldType::Decimal,
            [8_u8] => FieldType::Timestamp,
            [9_u8] => FieldType::Date,
            [10_u8] => FieldType::Bson,
            [11_u8] => FieldType::Null,
            _ => FieldType::Null,
        }
    }
}

impl Display for FieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum FieldType {
    UInt,
    Int,
    Float,
    Boolean,
    String,
    Text,
    Binary,
    Decimal,
    Timestamp,
    Date,
    Bson,
    Null,
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
    /// Unique identifier and version for this schema. This value is required only if the schema
    /// is represented by a valid entry in the schema registry. For nested schemas, this field
    /// is not applicable
    pub identifier: Option<SchemaIdentifier>,

    /// fields contains a list of FieldDefinition for all the fields that appear in a record.
    /// Not necessarily all these fields will end up in the final object structure stored in
    /// the cache. Some fields might only be used for indexing purposes only.
    pub fields: Vec<FieldDefinition>,

    /// Indexes of the fields representing values that will appear in the final object stored
    /// in the cache
    pub values: Vec<usize>,

    /// Indexes of the fields forming the primary key for this schema. If the value is empty
    /// only Insert Operation are supported. Updates and Deletes are not supported without a
    /// primary key definition
    pub primary_index: Vec<usize>,
}

impl Schema {
    pub fn empty() -> Schema {
        Self {
            identifier: None,
            fields: Vec::new(),
            values: Vec::new(),
            primary_index: Vec::new(),
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

    pub fn get_field_index(&self, name: &str) -> Result<(usize, &FieldDefinition), TypeError> {
        let r = self
            .fields
            .iter()
            .enumerate()
            .find(|f| f.1.name.as_str() == name);
        match r {
            Some(v) => Ok(v),
            _ => Err(TypeError::InvalidFieldName(name.to_string())),
        }
    }

    pub fn get_id(&self) -> u32 {
        self.identifier.clone().unwrap().id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(crate = "self::serde")]
pub enum SortDirection {
    #[serde(rename = "asc")]
    Ascending,
    #[serde(rename = "desc")]
    Descending,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum IndexDefinition {
    /// The sorted inverted index, supporting `Eq` filter on multiple fields and `LT`, `LTE`, `GT`, `GTE` filter on at most one field.
    SortedInverted(Vec<(usize, SortDirection)>),
    /// Full text index, supporting `Contains`, `MatchesAny` and `MatchesAll` filter on exactly one field.
    FullText(usize),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Record {
    /// Schema implemented by this Record
    pub schema_id: Option<SchemaIdentifier>,
    /// List of values, following the definitions of `fields` of the asscoiated schema
    pub values: Vec<Field>,
}
impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("values", &self.values)
            .finish()
    }
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

    pub fn iter(&self) -> core::slice::Iter<'_, Field> {
        self.values.iter()
    }

    pub fn set_value(&mut self, idx: usize, value: Field) {
        self.values[idx] = value;
    }

    pub fn get_value(&self, idx: usize) -> Result<&Field, TypeError> {
        match self.values.get(idx) {
            Some(f) => Ok(f),
            _ => Err(TypeError::InvalidFieldIndex(idx)),
        }
    }

    pub fn get_key(&self, indexes: &Vec<usize>) -> Result<Vec<u8>, TypeError> {
        let mut tot_size = 0_usize;
        let mut buffers = Vec::<Vec<u8>>::with_capacity(indexes.len());
        for i in indexes {
            let bytes = self.values[*i].to_bytes()?;
            tot_size += bytes.len();
            buffers.push(bytes);
        }

        let mut res_buffer = Vec::<u8>::with_capacity(tot_size);
        for i in buffers {
            res_buffer.extend(i);
        }
        Ok(res_buffer)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct OperationEvent {
    pub seq_no: u64,
    pub operation: Operation,
}

impl OperationEvent {
    pub fn new(seq_no: u64, operation: Operation) -> Self {
        Self { seq_no, operation }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Copy)]
pub struct Commit {
    pub seq_no: u64,
    pub lsn: u64,
}

impl Commit {
    pub fn new(seq_no: u64, lsn: u64) -> Self {
        Self { seq_no, lsn }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum Operation {
    Delete { old: Record },
    Insert { new: Record },
    Update { old: Record, new: Record },
}
