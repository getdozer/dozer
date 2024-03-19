use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, NaiveDate, NaiveDateTime, Offset, Utc},
    json_types::{serde_json_to_json_value, JsonValue},
    rust_decimal::Decimal,
    serde_json,
    types::{DozerDuration, DozerPoint, Field, FieldType, TimeUnit},
};
use geozero::{wkb, GeomProcessor};
use mysql_async::{Row, Value};
use std::time::Duration;

use crate::MySQLConnectorError;

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

pub fn get_field_type_for_sql_type(sql_data_type: &sqlparser::ast::DataType) -> FieldType {
    use sqlparser::ast::DataType;
    match sql_data_type {
        DataType::Character(_)
        | DataType::Char(_)
        | DataType::String(_)
        | DataType::Enum(_)
        | DataType::Set(_) => FieldType::String,
        DataType::CharacterVarying(_)
        | DataType::CharVarying(_)
        | DataType::Varchar(_)
        | DataType::Nvarchar(_)
        | DataType::Text => FieldType::Text,
        DataType::Uuid
        | DataType::CharacterLargeObject(_)
        | DataType::CharLargeObject(_)
        | DataType::Clob(_)
        | DataType::Regclass
        | DataType::Custom(_, _)
        | DataType::Array(_)
        | DataType::Struct(_) => unreachable!("MySQL does not support this type: {sql_data_type}"),
        DataType::Binary(_)
        | DataType::Varbinary(_)
        | DataType::Blob(_)
        | DataType::Bytes(_)
        | DataType::Bytea => FieldType::Binary,
        DataType::Numeric(_)
        | DataType::Decimal(_)
        | DataType::BigNumeric(_)
        | DataType::BigDecimal(_)
        | DataType::Dec(_) => FieldType::Decimal,
        DataType::Float(_)
        | DataType::Float4
        | DataType::Float64
        | DataType::Real
        | DataType::Float8
        | DataType::Double
        | DataType::DoublePrecision => FieldType::Float,
        DataType::TinyInt(_)
        | DataType::Int2(_)
        | DataType::SmallInt(_)
        | DataType::MediumInt(_)
        | DataType::Int(_)
        | DataType::Int4(_)
        | DataType::Int64
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::Int8(_) => FieldType::Int,
        DataType::UnsignedTinyInt(_)
        | DataType::UnsignedInt2(_)
        | DataType::UnsignedSmallInt(_)
        | DataType::UnsignedMediumInt(_)
        | DataType::UnsignedInt(_)
        | DataType::UnsignedInt4(_)
        | DataType::UnsignedInteger(_)
        | DataType::UnsignedBigInt(_)
        | DataType::UnsignedInt8(_) => FieldType::UInt,
        DataType::Bool | DataType::Boolean => FieldType::Boolean,
        DataType::Date => FieldType::Date,
        DataType::Time(_, _) | DataType::Interval => FieldType::Duration,
        DataType::Datetime(_) | DataType::Timestamp(_, _) => FieldType::Timestamp,
        DataType::JSON => FieldType::Json,
    }
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
                    Field::Timestamp(DateTime::from_naive_utc_and_offset(date_time, Utc.fix()))
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
                    wkb::process_wkb_geom(&mut wkb_point, &mut wkb_processor)?;
                    let point = wkb_processor.point.unwrap();
                    Field::Point(point)
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

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_common::{chrono::NaiveTime, Value};
    use std::time::Duration;

    #[test]
    fn test_field_types() {
        let supported_types = [
            // (mysql type, dozer type)
            ("int", FieldType::Int),
            ("int(8) unsigned zerofill", FieldType::UInt),
            ("tinyint", FieldType::Int),
            ("smallint(4)", FieldType::Int),
            ("mediumint", FieldType::Int),
            ("bigint unsigned", FieldType::UInt),
            ("year", FieldType::Int),
            ("decimal(5, 2)", FieldType::Decimal),
            ("decimal(10, 0) unsigned", FieldType::Decimal),
            ("float", FieldType::Float),
            ("double", FieldType::Float),
            ("timestamp(4)", FieldType::Timestamp),
            ("time(3)", FieldType::Duration),
            ("date", FieldType::Date),
            ("datetime(6)", FieldType::Timestamp),
            ("year", FieldType::Int),
            ("char", FieldType::String),
            ("varchar(20)", FieldType::Text),
            ("text", FieldType::Text),
            ("tinytext", FieldType::Text),
            ("mediumtext", FieldType::Text),
            ("longtext", FieldType::Text),
            ("binary(5)", FieldType::Binary),
            ("varbinary(10)", FieldType::Binary),
            ("blob", FieldType::Binary),
            ("tinyblob", FieldType::Binary),
            ("mediumblob", FieldType::Binary),
            ("longblob", FieldType::Binary),
            ("enum('a','b','c')", FieldType::String),
            ("set('1','2','3')", FieldType::String),
            ("json", FieldType::Json),
            ("point", FieldType::Point),
        ];

        let unsupported_types = [
            "null",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geomcollection",
            "geometry",
            "some fictional type",
            "int array",
        ];

        for (mysql_type, expected_field_type) in supported_types {
            let result = get_field_type_for_mysql_column_type(mysql_type);
            assert!(result.is_ok(), "unexpected error {result:?}");
            let actual_field_type = result.unwrap();
            assert_eq!(
                actual_field_type, expected_field_type,
                "expected {expected_field_type:?}; got {actual_field_type:?} for mysql type {mysql_type}"
            );
        }

        for mysql_type in unsupported_types {
            let result = get_field_type_for_mysql_column_type(mysql_type);
            assert!(result.is_err(), "expected an error; got {result:?}");
        }
    }

    #[test]
    fn test_field_conversion() {
        assert_eq!(
            Field::UInt(0),
            Value::UInt(0).into_field(&FieldType::UInt).unwrap()
        );
        assert_eq!(
            Field::U128(1),
            Value::UInt(1).into_field(&FieldType::U128).unwrap()
        );
        assert_eq!(
            Field::Int(2),
            Value::Int(2).into_field(&FieldType::Int).unwrap()
        );
        assert_eq!(
            Field::I128(3),
            Value::Int(3).into_field(&FieldType::I128).unwrap()
        );
        assert_eq!(
            Field::Float(4.0.into()),
            Value::Float(4.0).into_field(&FieldType::Float).unwrap()
        );
        assert_eq!(
            Field::Boolean(true),
            Value::Int(1).into_field(&FieldType::Boolean).unwrap()
        );
        assert_eq!(
            Field::String("6".into()),
            Value::Bytes(b"6".as_slice().into())
                .into_field(&FieldType::String)
                .unwrap()
        );
        assert_eq!(
            Field::Text("7".into()),
            Value::Bytes(b"7".as_slice().into())
                .into_field(&FieldType::Text)
                .unwrap()
        );
        assert_eq!(
            Field::Binary(b"8".as_slice().into()),
            Value::Bytes(b"8".as_slice().into())
                .into_field(&FieldType::Binary)
                .unwrap()
        );
        assert_eq!(
            Field::Decimal(9.into()),
            Value::Int(9).into_field(&FieldType::Decimal).unwrap()
        );
        assert_eq!(
            Field::Timestamp(DateTime::from_naive_utc_and_offset(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(2023, 8, 17).unwrap(),
                    NaiveTime::from_hms_micro_opt(10, 30, 0, 0).unwrap(),
                ),
                Utc.fix(),
            )),
            Value::Date(2023, 8, 17, 10, 30, 0, 0)
                .into_field(&FieldType::Timestamp)
                .unwrap()
        );
        assert_eq!(
            Field::Date(NaiveDate::from_ymd_opt(2023, 8, 11).unwrap()),
            Value::Date(2023, 8, 11, 0, 0, 0, 0)
                .into_field(&FieldType::Date)
                .unwrap()
        );
        assert_eq!(
            Field::Json(12.0.into()),
            Value::Bytes(b"12".as_slice().into())
                .into_field(&FieldType::Json)
                .unwrap()
        );
        assert_eq!(
            Field::Point((13.0, 0.0).into()),
            Value::Bytes(
                hex::decode("0000000001010000000000000000002a400000000000000000").unwrap(),
            )
            .into_field(&FieldType::Point)
            .unwrap()
        );
        assert_eq!(
            Field::Duration(DozerDuration(
                Duration::from_micros(14),
                TimeUnit::Microseconds,
            )),
            Value::Time(false, 0, 0, 0, 0, 14)
                .into_field(&FieldType::Duration)
                .unwrap()
        );

        assert_eq!(
            Field::Null,
            Value::NULL.into_field(&FieldType::Int).unwrap()
        );
        assert_eq!(
            Field::Null,
            None::<Value>.into_field(&FieldType::Int).unwrap()
        );

        assert!(
            Value::Bytes(hex::decode("0000000001010000000000000000002a40").unwrap())
                .into_field(&FieldType::Point)
                .is_err()
        );
    }
}
