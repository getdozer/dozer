use crate::errors::MySQLConnectorError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Offset, Utc};
use dozer_types::{
    json_types::{serde_json_to_json_value, JsonValue},
    rust_decimal::Decimal,
    serde_json,
    types::{DozerDuration, DozerPoint, Field, FieldType, TimeUnit},
};
use geozero::{wkb, GeomProcessor};
use mysql_common::{Row, Value};
use std::time::Duration;

pub fn get_field_type_for_mysql_column_type(
    column_type: &str,
) -> Result<FieldType, MySQLConnectorError> {
    let data_type = {
        let space = column_type.find(' ');
        let parenthesis = column_type.find('(');
        let end = {
            let max = column_type.len();
            std::cmp::min(space.unwrap_or(max), parenthesis.unwrap_or(max))
        };
        &column_type[0..end]
    };
    let is_array = column_type.ends_with(" array");
    let is_unsigned = column_type.contains(" unsigned");

    if is_array {
        return Err(MySQLConnectorError::UnsupportedFieldType(data_type.into()));
    }

    let field_type = match data_type {
        "decimal" => FieldType::Decimal,
        "int" | "tinyint" | "smallint" | "mediumint" | "bigint" => {
            if is_unsigned {
                FieldType::UInt
            } else {
                FieldType::Int
            }
        }
        "float" | "double" => FieldType::Float,
        "timestamp" => FieldType::Timestamp,
        "time" => FieldType::Duration,
        "year" => FieldType::Int,
        "date" => FieldType::Date,
        "datetime" => FieldType::Timestamp,
        "varchar" => FieldType::Text,
        "varbinary" => FieldType::Binary,
        "char" => FieldType::String,
        "binary" => FieldType::Binary,
        "tinyblob" => FieldType::Binary,
        "blob" => FieldType::Binary,
        "mediumblob" => FieldType::Binary,
        "longblob" => FieldType::Binary,
        "tinytext" => FieldType::Text,
        "text" => FieldType::Text,
        "mediumtext" => FieldType::Text,
        "longtext" => FieldType::Text,
        "point" => FieldType::Point,
        "json" => FieldType::Json,
        "bit" => FieldType::Int,
        "enum" => FieldType::String,
        "set" => FieldType::String,
        "null" | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon"
        | "geomcollection" | "geometry" => {
            Err(MySQLConnectorError::UnsupportedFieldType(data_type.into()))?
        }
        _ => Err(MySQLConnectorError::UnsupportedFieldType(data_type.into()))?,
    };

    Ok(field_type)
}

pub trait IntoFields<'a> {
    type Ctx: 'a;

    fn into_fields(self, ctx: Self::Ctx) -> Result<Vec<Field>, MySQLConnectorError>;
}

impl<'a> IntoFields<'a> for Row {
    type Ctx = &'a [FieldType];

    fn into_fields(self, field_types: &[FieldType]) -> Result<Vec<Field>, MySQLConnectorError> {
        let mut row = self;
        let mut fields = Vec::new();
        for i in 0..row.len() {
            let field = row
                .take::<Value, usize>(i)
                .into_field(field_types.get(i).unwrap())?;
            fields.push(field);
        }
        Ok(fields)
    }
}

pub trait IntoField<'a> {
    type Ctx: 'a;

    fn into_field(self, ctx: Self::Ctx) -> Result<Field, MySQLConnectorError>;
}

impl<'a> IntoField<'a> for Option<Value> {
    type Ctx = &'a FieldType;

    fn into_field(self, field_type: &FieldType) -> Result<Field, MySQLConnectorError> {
        if let Some(value) = self {
            value.into_field(field_type)
        } else {
            Ok(Field::Null)
        }
    }
}

impl<'a> IntoField<'a> for Value {
    type Ctx = &'a FieldType;

    fn into_field(self, field_type: &FieldType) -> Result<Field, MySQLConnectorError> {
        use mysql_common::value::convert::from_value_opt;
        use Value::*;

        let value = self;

        let field = if let NULL = value {
            Field::Null
        } else {
            match field_type {
                FieldType::UInt => Field::UInt(from_value_opt::<u64>(value)?),
                FieldType::U128 => Field::U128(from_value_opt::<u128>(value)?),
                FieldType::Int => Field::Int(from_value_opt::<i64>(value)?),
                FieldType::I128 => Field::I128(from_value_opt::<i128>(value)?),
                FieldType::Float => Field::Float(from_value_opt::<f64>(value)?.into()),
                FieldType::Boolean => Field::Boolean(from_value_opt::<bool>(value)?),
                FieldType::String => Field::String(from_value_opt::<String>(value)?),
                FieldType::Text => Field::Text(from_value_opt::<String>(value)?),
                FieldType::Binary => Field::Binary(from_value_opt::<Vec<u8>>(value)?),
                FieldType::Decimal => Field::Decimal(from_value_opt::<Decimal>(value)?),
                FieldType::Timestamp => {
                    let date_time = from_value_opt::<NaiveDateTime>(value)?;
                    Field::Timestamp(DateTime::from_utc(date_time, Utc.fix()))
                }
                FieldType::Date => Field::Date(from_value_opt::<NaiveDate>(value)?),
                FieldType::Json => {
                    let json =
                        serde_json_to_json_value(from_value_opt::<serde_json::Value>(value)?)?;
                    Field::Json(json)
                }
                FieldType::Point => {
                    let bytes = from_value_opt::<Vec<u8>>(value)?;
                    let (_srid, mut wkb_point) = bytes.as_slice().split_at(4);
                    let mut wkb_processor = PointProcessor::new();
                    wkb::process_wkb_geom(&mut wkb_point, &mut wkb_processor).unwrap();
                    if let Some(point) = wkb_processor.point {
                        Field::Point(point)
                    } else {
                        Err(mysql_common::FromValueError(Value::Bytes(bytes)))?
                    }
                }
                FieldType::Duration => Field::Duration(DozerDuration(
                    from_value_opt::<Duration>(value)?,
                    TimeUnit::Microseconds,
                )),
            }
        };

        Ok(field)
    }
}

pub trait IntoJsonValue {
    fn into_json_value(self) -> Result<JsonValue, MySQLConnectorError>;
}

struct PointProcessor {
    point: Option<DozerPoint>,
}

impl PointProcessor {
    pub fn new() -> Self {
        Self { point: None }
    }
}

impl GeomProcessor for PointProcessor {
    fn dimensions(&self) -> geozero::CoordDimensions {
        geozero::CoordDimensions::xy()
    }

    fn xy(&mut self, x: f64, y: f64, _idx: usize) -> geozero::error::Result<()> {
        self.point = Some((x, y).into());
        Ok(())
    }
}
