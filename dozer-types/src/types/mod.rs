use chrono::{DateTime, FixedOffset};
use geo::{point, GeodesicDistance, Point};
use ordered_float::OrderedFloat;
use std::array::TryFromSliceError;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::str::FromStr;

use crate::errors::types::TypeError;
use crate::node::OpIdentifier;
use prettytable::{Cell, Row, Table};
use serde::{self, Deserialize, Serialize};

pub mod field;

#[cfg(test)]
mod tests;

use crate::errors::internal::BoxedError;
use crate::errors::types::TypeError::InvalidFieldValue;
pub use field::{field_test_cases, Field, FieldType, DATE_FORMAT};

#[derive(
    Clone,
    Serialize,
    Deserialize,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    bincode::Encode,
    bincode::Decode,
)]
pub enum SourceDefinition {
    Table {
        connection: String,
        name: String,
    },
    Alias {
        name: String,
    },
    #[default]
    Dynamic,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FieldDefinition {
    pub name: String,
    pub typ: FieldType,
    pub nullable: bool,
    #[serde(default)]
    pub source: SourceDefinition,
    pub description: Option<String>,
}

impl FieldDefinition {
    pub fn new(name: String, typ: FieldType, nullable: bool, source: SourceDefinition) -> Self {
        Self {
            name,
            typ,
            nullable,
            source,
            description: None,
        }
    }

    pub fn check_from(&self, table_name: String) -> bool {
        match &self.source {
            SourceDefinition::Table { name, .. } => *name == table_name,
            SourceDefinition::Alias { name } => *name == table_name,
            SourceDefinition::Dynamic => false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct Schema {
    /// fields contains a list of FieldDefinition for all the fields that appear in a record.
    /// Not necessarily all these fields will end up in the final object structure stored in
    /// the cache. Some fields might only be used for indexing purposes only.
    pub fields: Vec<FieldDefinition>,

    /// Indexes of the fields forming the primary key for this schema. If the value is empty
    /// only Insert Operation are supported. Updates and Deletes are not supported without a
    /// primary key definition
    #[serde(default)]
    pub primary_index: Vec<usize>,
}

impl Schema {
    pub fn new() -> Schema {
        Self::default()
    }

    pub fn field(&mut self, f: FieldDefinition, pk: bool) -> &mut Self {
        self.fields.push(f);
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

    pub fn print(&self) -> Table {
        let mut table = Table::new();
        table.add_row(row!["Field", "Type", "Nullable", "PK"]);
        for (index, field) in self.fields.iter().enumerate() {
            table.add_row(row![
                field.name,
                format!("{:?}", field.typ),
                field.nullable,
                self.primary_index.contains(&index)
            ]);
        }
        table
    }

    /// Returns if this schema is append only.
    ///
    /// Append only schemas enable additional optimizations, however, the connectors and processors haven't properly implemented this yet.
    pub fn is_append_only(&self) -> bool {
        false
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let table = self.print();
        table.fmt(f)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum IndexDefinition {
    /// The sorted inverted index, supporting `Eq` filter on multiple fields and `LT`, `LTE`, `GT`, `GTE` filter on at most one field.
    SortedInverted(Vec<usize>),
    /// Full text index, supporting `Contains`, `MatchesAny` and `MatchesAll` filter on exactly one field.
    FullText(usize),
}

pub type SchemaWithIndex = (Schema, Vec<IndexDefinition>);

pub type Timestamp = DateTime<FixedOffset>;

#[derive(
    Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode,
)]
pub struct Lifetime {
    #[bincode(with_serde)]
    pub reference: Timestamp,
    pub duration: std::time::Duration,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    Default,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Record {
    /// List of values, following the definitions of `fields` of the associated schema
    pub values: Vec<Field>,

    /// Time To Live for this record. If the value is None, the record will never expire.
    pub lifetime: Option<Lifetime>,
}

impl Record {
    pub fn new(values: Vec<Field>) -> Record {
        Record {
            values,
            lifetime: None,
        }
    }

    pub fn nulls_from_schema(schema: &Schema) -> Record {
        Self::nulls(schema.fields.len())
    }

    pub fn nulls(size: usize) -> Record {
        Record {
            values: vec![Field::Null; size],
            lifetime: None,
        }
    }

    pub fn values(&self) -> &[Field] {
        &self.values
    }

    pub fn iter(&self) -> core::slice::Iter<'_, Field> {
        self.values.iter()
    }

    pub fn set_value(&mut self, idx: usize, value: Field) {
        self.values[idx] = value;
    }

    pub fn push_value(&mut self, value: Field) {
        self.values.push(value);
    }

    pub fn get_value(&self, idx: usize) -> Result<&Field, TypeError> {
        match self.values.get(idx) {
            Some(f) => Ok(f),
            _ => Err(TypeError::InvalidFieldIndex(idx)),
        }
    }

    pub fn appended(existing: &Record, additional: &[Field]) -> Self {
        let mut values = existing.values.clone();
        values.extend_from_slice(additional);
        Self::new(values)
    }

    pub fn get_key_fields(&self, schema: &Schema) -> Vec<Field> {
        self.get_fields_by_indexes(&schema.primary_index)
    }

    pub fn get_fields_by_indexes(&self, indexes: &[usize]) -> Vec<Field> {
        debug_assert!(!&indexes.is_empty(), "Primary key indexes cannot be empty");

        let mut fields = Vec::with_capacity(indexes.len());
        for i in indexes {
            fields.push(self.values[*i].clone());
        }
        fields
    }

    pub fn get_key(&self, indexes: &Vec<usize>) -> Vec<u8> {
        debug_assert!(!indexes.is_empty(), "Primary key indexes cannot be empty");

        let mut tot_size = 0_usize;
        let mut buffers = Vec::<Vec<u8>>::with_capacity(indexes.len());
        for i in indexes {
            let bytes = self.values[*i].encode();
            tot_size += bytes.len();
            buffers.push(bytes);
        }

        let mut res_buffer = Vec::<u8>::with_capacity(tot_size);
        for i in buffers {
            res_buffer.extend(i);
        }
        res_buffer
    }

    pub fn set_lifetime(&mut self, lifetime: Option<Lifetime>) {
        self.lifetime = lifetime;
    }

    pub fn get_lifetime(&self) -> Option<Lifetime> {
        self.lifetime.clone()
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let v = self
            .values
            .iter()
            .map(|f| Cell::new(&f.to_string()))
            .collect::<Vec<Cell>>();

        let mut table = Table::new();
        table.add_row(Row::new(v));
        table.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
/// A CDC event.
pub enum Operation {
    Delete { old: Record },
    Insert { new: Record },
    Update { old: Record, new: Record },
    BatchInsert { new: Vec<Record> },
}

pub type PortHandle = u16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub struct TableOperation {
    pub id: Option<OpIdentifier>,
    pub op: Operation,
    /// For outputting an operation, node should fill the output port.
    /// For received operation, the port is the input port.
    /// Port mapping is done in forwarders.
    pub port: PortHandle,
}

impl TableOperation {
    pub fn without_id(op: Operation, port: PortHandle) -> Self {
        Self { id: None, op, port }
    }
}

// Helpful in interacting with external systems during ingestion and querying
// For example, nanoseconds can overflow.
#[derive(
    Clone,
    Copy,
    Serialize,
    Deserialize,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    bincode::Decode,
    bincode::Encode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum TimeUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Seconds => f.write_str("Seconds"),
            TimeUnit::Milliseconds => f.write_str("Milliseconds"),
            TimeUnit::Microseconds => f.write_str("Microseconds"),
            TimeUnit::Nanoseconds => f.write_str("Nanoseconds"),
        }
    }
}

impl FromStr for TimeUnit {
    type Err = TypeError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let error = || InvalidFieldValue {
            field_type: FieldType::Duration,
            nullable: false,
            value: str.to_string(),
        };
        let string = str.parse::<String>().map_err(|_| error())?;
        match string.as_str() {
            "Seconds" => Ok(TimeUnit::Seconds),
            "Milliseconds" => Ok(TimeUnit::Milliseconds),
            "Microseconds" => Ok(TimeUnit::Microseconds),
            "Nanoseconds" => Ok(TimeUnit::Nanoseconds),
            &_ => Err(error()),
        }
    }
}

impl TimeUnit {
    pub fn to_bytes(&self) -> [u8; 1] {
        match self {
            TimeUnit::Seconds => [0_u8],
            TimeUnit::Milliseconds => [1_u8],
            TimeUnit::Microseconds => [2_u8],
            TimeUnit::Nanoseconds => [3_u8],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BoxedError> {
        match bytes {
            [0_u8] => Ok(TimeUnit::Seconds),
            [1_u8] => Ok(TimeUnit::Milliseconds),
            [2_u8] => Ok(TimeUnit::Microseconds),
            [3_u8] => Ok(TimeUnit::Nanoseconds),
            _ => Err(BoxedError::from("Unsupported unit".to_string())),
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    Hash,
    bincode::Encode,
    bincode::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DozerDuration(pub std::time::Duration, pub TimeUnit);

impl Ord for DozerDuration {
    fn cmp(&self, other: &Self) -> Ordering {
        std::time::Duration::cmp(&self.0, &other.0)
    }
}

impl PartialOrd for DozerDuration {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for DozerDuration {
    type Err = TypeError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let error = || InvalidFieldValue {
            field_type: FieldType::Duration,
            nullable: false,
            value: str.to_string(),
        };
        let val = str.parse::<u64>().map_err(|_| error())?;
        Ok(Self(
            std::time::Duration::from_nanos(val),
            TimeUnit::Nanoseconds,
        ))
    }
}

impl DozerDuration {
    pub fn to_bytes(&self) -> [u8; 17] {
        let mut result = [0_u8; 17];
        result[0..1].copy_from_slice(&self.1.to_bytes());
        result[1..17].copy_from_slice(&self.0.as_nanos().to_be_bytes());
        result
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TryFromSliceError> {
        let unit = TimeUnit::from_bytes(bytes[0..1].try_into()?).unwrap();
        let val =
            std::time::Duration::from_nanos(u128::from_be_bytes(bytes[1..17].try_into()?) as u64);

        Ok(DozerDuration(val, unit))
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    Hash,
    bincode::Encode,
    bincode::Decode,
)]
pub struct DozerPoint(#[bincode(with_serde)] pub Point<OrderedFloat<f64>>);

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for DozerPoint {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let x = self::field::arbitrary_float(u)?;
        let y = self::field::arbitrary_float(u)?;

        Ok(Self(geo::Point::new(x, y)))
    }
}

impl GeodesicDistance<OrderedFloat<f64>> for DozerPoint {
    fn geodesic_distance(&self, rhs: &Self) -> OrderedFloat<f64> {
        let f = point! { x: self.0.x().0, y: self.0.y().0 };
        let t = point! { x: rhs.0.x().0, y: rhs.0.y().0 };
        OrderedFloat(f.geodesic_distance(&t))
    }
}

impl Ord for DozerPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0.x() == other.0.x() && self.0.y() == other.0.y() {
            Ordering::Equal
        } else if self.0.x() > other.0.x()
            || (self.0.x() == other.0.x() && self.0.y() > other.0.y())
        {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl PartialOrd for DozerPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for DozerPoint {
    type Err = TypeError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let error = || InvalidFieldValue {
            field_type: FieldType::Point,
            nullable: false,
            value: str.to_string(),
        };

        let s = str.replace('(', "");
        let s = s.replace(')', "");
        let mut cs = s.split(',');
        let x = cs
            .next()
            .ok_or_else(error)?
            .parse::<f64>()
            .map_err(|_| error())?;
        let y = cs
            .next()
            .ok_or_else(error)?
            .parse::<f64>()
            .map_err(|_| error())?;
        Ok(Self(Point::from((OrderedFloat(x), OrderedFloat(y)))))
    }
}

impl From<(f64, f64)> for DozerPoint {
    fn from((x, y): (f64, f64)) -> Self {
        Self(point! {x: OrderedFloat(x), y: OrderedFloat(y)})
    }
}

impl Display for DozerPoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self.0.x_y()))
    }
}

impl DozerPoint {
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut result = [0_u8; 16];
        result[0..8].copy_from_slice(&self.0.x().to_be_bytes());
        result[8..16].copy_from_slice(&self.0.y().to_be_bytes());
        result
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TryFromSliceError> {
        let x = f64::from_be_bytes(bytes[0..8].try_into()?);
        let y = f64::from_be_bytes(bytes[8..16].try_into()?);

        Ok(DozerPoint::from((x, y)))
    }
}
