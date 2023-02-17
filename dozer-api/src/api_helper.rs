use crate::auth::Access;
use crate::errors::{ApiError, AuthError};
use dozer_cache::cache::RecordWithId;
use dozer_cache::cache::{expression::QueryExpression, index};
use dozer_cache::errors::CacheError;
use dozer_cache::{AccessFilter, CacheReader};
use dozer_types::chrono::SecondsFormat;
use dozer_types::errors::types::{DeserializationError, TypeError};
use dozer_types::indexmap::IndexMap;
use dozer_types::json_value_to_field;
use dozer_types::serde_json::{Map, Value};
use dozer_types::types::{Field, FieldType, Schema, DATE_FORMAT};

pub struct ApiHelper<'a> {
    reader: &'a CacheReader,
    access_filter: AccessFilter,
    endpoint_name: &'a str,
}
impl<'a> ApiHelper<'a> {
    pub fn new(
        reader: &'a CacheReader,
        endpoint_name: &'a str,
        access: Option<Access>,
    ) -> Result<Self, ApiError> {
        let access = access.unwrap_or(Access::All);

        // Define Access Filter based on token
        let access_filter = match access {
            // No access filter.
            Access::All => AccessFilter {
                filter: None,
                fields: vec![],
            },

            Access::Custom(mut access_filters) => {
                if let Some(access_filter) = access_filters.remove(endpoint_name) {
                    access_filter
                } else {
                    return Err(ApiError::ApiAuthError(AuthError::InvalidToken));
                }
            }
        };

        Ok(Self {
            reader,
            access_filter,
            endpoint_name,
        })
    }

    /// Get a single record by json string as primary key
    pub fn get_record(&self, key: &str) -> Result<IndexMap<String, Value>, CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(self.endpoint_name)?
            .0;

        let key = if schema.primary_index.is_empty() {
            json_str_to_field(key, dozer_types::types::FieldType::UInt, false)
                .map_err(CacheError::Type)
        } else if schema.primary_index.len() == 1 {
            let field = &schema.fields[schema.primary_index[0]];
            json_str_to_field(key, field.typ, field.nullable).map_err(CacheError::Type)
        } else {
            Err(CacheError::Query(
                dozer_cache::errors::QueryError::MultiIndexFetch(key.to_string()),
            ))
        }?;

        let key = index::get_primary_key(&[0], &[key]);
        let record = self.reader.get(&key, &self.access_filter)?;

        record_to_map(record, &schema).map_err(CacheError::Type)
    }

    pub fn get_records_count(self, mut exp: QueryExpression) -> Result<usize, CacheError> {
        self.reader
            .count(self.endpoint_name, &mut exp, self.access_filter)
    }

    /// Get multiple records
    pub fn get_records_map(
        self,
        exp: QueryExpression,
    ) -> Result<Vec<IndexMap<String, Value>>, CacheError> {
        let mut maps = vec![];
        let (schema, records) = self.get_records(exp)?;
        for record in records.into_iter() {
            let map = record_to_map(record, &schema)?;
            maps.push(map);
        }
        Ok(maps)
    }
    /// Get multiple records
    pub fn get_records(
        self,
        mut exp: QueryExpression,
    ) -> Result<(Schema, Vec<RecordWithId>), CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(self.endpoint_name)?
            .0;
        let records = self
            .reader
            .query(self.endpoint_name, &mut exp, self.access_filter)?;

        Ok((schema, records))
    }

    /// Get schema
    pub fn get_schema(&self) -> Result<Schema, CacheError> {
        let schema = self
            .reader
            .get_schema_and_indexes_by_name(self.endpoint_name)?
            .0;
        Ok(schema)
    }
}

/// Used in REST APIs for converting to JSON
fn record_to_map(
    record: RecordWithId,
    schema: &Schema,
) -> Result<IndexMap<String, Value>, TypeError> {
    let mut map = IndexMap::new();

    for (field_def, field) in schema.fields.iter().zip(record.record.values) {
        let val = field_to_json_value(field);
        map.insert(field_def.name.clone(), val);
    }

    map.insert("__dozer_record_id".to_string(), Value::from(record.id));
    map.insert(
        "__dozer_record_version".to_string(),
        Value::from(record.record.version),
    );

    Ok(map)
}

fn convert_x_y_to_object((x, y): &(f64, f64)) -> Value {
    let mut m = Map::new();
    m.insert("x".to_string(), Value::from(*x));
    m.insert("y".to_string(), Value::from(*y));
    Value::Object(m)
}

/// Used in REST APIs for converting raw value back and forth.
///
/// Should be consistent with `convert_cache_type_to_schema_type`.
fn field_to_json_value(field: Field) -> Value {
    match field {
        Field::UInt(n) => Value::from(n),
        Field::Int(n) => Value::from(n),
        Field::Float(n) => Value::from(n.0),
        Field::Boolean(b) => Value::from(b),
        Field::String(s) => Value::from(s),
        Field::Text(n) => Value::from(n),
        Field::Binary(b) => Value::from(b),
        Field::Decimal(n) => Value::String(n.to_string()),
        Field::Timestamp(ts) => Value::String(ts.to_rfc3339_opts(SecondsFormat::Millis, true)),
        Field::Date(n) => Value::String(n.format(DATE_FORMAT).to_string()),
        Field::Bson(b) => Value::from(b),
        Field::Point(point) => convert_x_y_to_object(&point.0.x_y()),
        Field::Null => Value::Null,
    }
}

fn json_str_to_field(value: &str, typ: FieldType, nullable: bool) -> Result<Field, TypeError> {
    let value = dozer_types::serde_json::from_str(value)
        .map_err(|e| TypeError::DeserializationError(DeserializationError::Json(e)))?;
    json_value_to_field(value, typ, nullable)
}

#[cfg(test)]
mod tests {
    use dozer_types::types::DozerPoint;
    use dozer_types::{
        chrono::{NaiveDate, Offset, TimeZone, Utc},
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
    };

    use super::*;

    fn test_field_conversion(field_type: FieldType, field: Field) {
        // Convert the field to a JSON value.
        let value = field_to_json_value(field.clone());

        // Convert the JSON value back to a Field.
        let deserialized = json_value_to_field(value, field_type, true).unwrap();

        assert_eq!(deserialized, field, "must be equal");
    }

    #[test]
    fn test_field_types_json_conversion() {
        let fields = vec![
            (FieldType::Int, Field::Int(-1)),
            (FieldType::UInt, Field::UInt(1)),
            (FieldType::Float, Field::Float(OrderedFloat(1.1))),
            (FieldType::Boolean, Field::Boolean(true)),
            (FieldType::String, Field::String("a".to_string())),
            (FieldType::Binary, Field::Binary(b"asdf".to_vec())),
            (FieldType::Decimal, Field::Decimal(Decimal::new(202, 2))),
            (
                FieldType::Timestamp,
                Field::Timestamp(Utc.fix().with_ymd_and_hms(2001, 1, 1, 0, 4, 0).unwrap()),
            ),
            (
                FieldType::Date,
                Field::Date(NaiveDate::from_ymd_opt(2022, 11, 24).unwrap()),
            ),
            (
                FieldType::Bson,
                Field::Bson(vec![
                    // BSON representation of `{"abc":"foo"}`
                    123, 34, 97, 98, 99, 34, 58, 34, 102, 111, 111, 34, 125,
                ]),
            ),
            (FieldType::Text, Field::Text("lorem ipsum".to_string())),
            (
                FieldType::Point,
                Field::Point(DozerPoint::from((3.234, 4.567))),
            ),
        ];
        for (field_type, field) in fields {
            test_field_conversion(field_type, field);
        }
    }

    #[test]
    fn test_nullable_field_conversion() {
        assert_eq!(
            json_str_to_field("null", FieldType::Int, true).unwrap(),
            Field::Null
        );
        assert!(json_str_to_field("null", FieldType::Int, false).is_err());
    }
}
