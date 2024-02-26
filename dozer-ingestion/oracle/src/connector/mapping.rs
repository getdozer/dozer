use std::{collections::HashMap, str::FromStr};

use dozer_ingestion_connector::{
    dozer_types::{
        chrono::{DateTime, NaiveDate, Utc},
        log::warn,
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
        thiserror,
        types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
    },
    CdcType, SourceSchema,
};
use oracle::Row;

use super::{join::Column, Error};

#[derive(Debug, Clone, Copy)]
pub struct MappedColumn {
    pub typ: FieldType,
    pub nullable: bool,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum DataTypeError {
    #[error("unsupported data type: {0}")]
    UnsupportedDataType(String),
    #[error("column {schema}.{table_name}.{column_name} has null data type")]
    ColumnDataTypeIsNull {
        schema: String,
        table_name: String,
        column_name: String,
    },
}

fn map_data_type(
    schema: &str,
    table_name: &str,
    column_name: &str,
    data_type: Option<&str>,
    nullable: Option<&str>,
    precision: Option<i64>,
    scale: Option<i64>,
) -> Result<MappedColumn, DataTypeError> {
    let data_type = data_type.ok_or_else(|| DataTypeError::ColumnDataTypeIsNull {
        schema: schema.to_string(),
        table_name: table_name.to_string(),
        column_name: column_name.to_string(),
    })?;
    let typ = if data_type.starts_with("TIMESTAMP") {
        FieldType::Timestamp
    } else {
        match data_type {
            "VARCHAR2" => Ok(FieldType::String),
            "NVARCHAR2" => unimplemented!("convert NVARCHAR2 to String"),
            "INTEGER" => Ok(FieldType::I128),
            "NUMBER" => match (precision, scale) {
                (Some(precision), Some(0)) if precision <= 19 => Ok(FieldType::Int),
                (_, Some(0)) => Ok(FieldType::I128),
                _ => Ok(FieldType::Decimal),
            },
            "FLOAT" => Ok(FieldType::Float),
            "DATE" => Ok(FieldType::Date),
            "BINARY_FLOAT" => Ok(FieldType::Float),
            "BINARY_DOUBLE" => Ok(FieldType::Float),
            "RAW" => Ok(FieldType::Binary),
            "ROWID" => Ok(FieldType::String),
            "CHAR" => Ok(FieldType::String),
            "NCHAR" => unimplemented!("convert NCHAR to String"),
            "CLOB" => Ok(FieldType::String),
            "NCLOB" => unimplemented!("convert NCLOB to String"),
            "BLOB" => Ok(FieldType::Binary),
            other => Err(DataTypeError::UnsupportedDataType(other.to_string())),
        }?
    };
    let nullable = nullable != Some("N");
    Ok(MappedColumn { typ, nullable })
}

pub fn map_row(schema: &Schema, row: Row) -> Result<Record, Error> {
    if schema.fields.len() != row.sql_values().len() {
        return Err(Error::ColumnCountMismatch {
            expected: schema.fields.len(),
            actual: row.sql_values().len(),
        });
    }

    let values = schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| map_field(index, field, &row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Record::new(values))
}

fn map_field(index: usize, field: &FieldDefinition, row: &Row) -> Result<Field, Error> {
    Ok(match (field.typ, field.nullable) {
        (FieldType::Int, true) => row
            .get::<_, Option<i64>>(index)?
            .map_or(Field::Null, Field::Int),
        (FieldType::Int, false) => Field::Int(row.get(index)?),
        (FieldType::UInt, true) => row
            .get::<_, Option<u64>>(index)?
            .map_or(Field::Null, Field::UInt),
        (FieldType::UInt, false) => Field::UInt(row.get(index)?),
        (FieldType::Float, true) => row
            .get::<_, Option<f64>>(index)?
            .map_or(Field::Null, |value| Field::Float(OrderedFloat(value))),
        (FieldType::Float, false) => Field::Float(OrderedFloat(row.get(index)?)),
        (FieldType::Decimal, true) => match row.get::<_, Option<String>>(index)? {
            Some(decimal) => Field::Decimal(Decimal::from_str(&decimal)?),
            None => Field::Null,
        },
        (FieldType::Decimal, false) => {
            Field::Decimal(Decimal::from_str(&row.get::<_, String>(index)?)?)
        }
        (FieldType::String, true) => row
            .get::<_, Option<String>>(index)?
            .map_or(Field::Null, Field::String),
        (FieldType::String, false) => Field::String(row.get(index)?),
        (FieldType::Binary, true) => row
            .get::<_, Option<Vec<u8>>>(index)?
            .map_or(Field::Null, Field::Binary),
        (FieldType::Binary, false) => Field::Binary(row.get(index)?),
        (FieldType::Date, true) => row
            .get::<_, Option<NaiveDate>>(index)?
            .map_or(Field::Null, Field::Date),
        (FieldType::Date, false) => Field::Date(row.get(index)?),
        (FieldType::Timestamp, true) => row
            .get::<_, Option<DateTime<Utc>>>(index)?
            .map_or(Field::Null, |value| Field::Timestamp(value.fixed_offset())),
        (FieldType::Timestamp, false) => {
            Field::Timestamp(row.get::<_, DateTime<Utc>>(index)?.fixed_offset())
        }
        _ => unreachable!(),
    })
}

#[derive(Debug, Clone)]
pub struct MappedColumnResult {
    pub is_primary_key: bool,
    pub is_used: bool,
    pub map_result: Result<MappedColumn, DataTypeError>,
}

pub type ColumnMap = HashMap<String, MappedColumnResult>;

pub fn map_tables(
    tables: HashMap<(String, String), Vec<Column>>,
) -> HashMap<(String, String), ColumnMap> {
    tables
        .into_iter()
        .map(|((schema, table_name), columns)| {
            let column_map = map_columns(&schema, &table_name, columns);
            ((schema, table_name), column_map)
        })
        .collect()
}

fn map_columns(schema: &str, table_name: &str, columns: Vec<Column>) -> ColumnMap {
    columns
        .into_iter()
        .map(|column| {
            let map_result = map_data_type(
                schema,
                table_name,
                &column.name,
                column.data_type.as_deref(),
                column.nullable.as_deref(),
                column.precision,
                column.scale,
            );
            (
                column.name,
                MappedColumnResult {
                    is_primary_key: column.is_primary_key,
                    is_used: false,
                    map_result,
                },
            )
        })
        .collect()
}

pub fn decide_schema(
    connection: &str,
    schema: Option<String>,
    table_name: String,
    column_names: &[String],
    mut columns: ColumnMap,
) -> Result<SourceSchema, Error> {
    let mut fields = vec![];
    let mut primary_index = vec![];
    for column_name in column_names {
        let Some(column) = columns.get_mut(column_name) else {
            return Err(Error::ColumnNotFound {
                schema,
                table_name,
                column_name: column_name.clone(),
            });
        };

        column.is_used = true;
        if column.is_primary_key {
            primary_index.push(fields.len());
        }

        match &column.map_result {
            Ok(column) => fields.push(FieldDefinition {
                name: column_name.clone(),
                typ: column.typ,
                nullable: column.nullable,
                source: SourceDefinition::Table {
                    connection: connection.to_string(),
                    name: table_name.clone(),
                },
            }),
            Err(err) => return Err(Error::DataType(err.clone())),
        }
    }

    if let Some((column_name, _)) = columns
        .iter()
        .find(|(_, column)| !column.is_used && column.is_primary_key)
    {
        warn!(
            "Primary key column {} of table {} in connection {} is not used. Dropping primary key.",
            column_name, table_name, connection
        );
        primary_index.clear();
    }

    Ok(SourceSchema {
        schema: Schema {
            fields,
            primary_index,
        },
        cdc_type: CdcType::OnlyPK, // Doesn't matter
    })
}
