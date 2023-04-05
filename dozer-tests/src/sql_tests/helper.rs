use crate::read_csv::read_csv;

use super::SqlMapper;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Record, Schema, SchemaIdentifier, SourceDefinition,
};
use sqlparser::ast::{Expr, ObjectName};
use std::error::Error;
use std::str;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

#[macro_export]
macro_rules! match_type {
   ($obj:expr, $($field_typ:pat => $block:expr),*) => {
       match $obj {
           $($field_typ => $block),*
       }
   }
}
#[macro_export]
macro_rules! convert_type {
    ($typ:expr, $field_def:expr, $row:expr, $idx: expr) => {
        if $field_def.nullable {
            $row.get($idx).map_or(Field::Null, $typ)
        } else {
            $typ($row.get($idx)?)
        }
    };
}
pub fn get_table_create_sql(name: &str, schema: Schema) -> String {
    let columns = schema
        .fields
        .iter()
        .map(|f| {
            let typ = match f.typ {
                FieldType::UInt => "integer",
                FieldType::Int => "integer",
                FieldType::String => "string",
                FieldType::Text => "text",
                FieldType::Float | FieldType::Binary => "numeric",
                FieldType::Timestamp => "timestamp",
                FieldType::Boolean => "bool",
                typ => panic!("unsupported type {typ:?}"),
            };
            format!(
                "{} {}",
                f.name.replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
                typ
            )
        })
        .collect::<Vec<String>>()
        .join(",");
    let creation_sql = format!("CREATE TABLE {name} ({columns})");
    creation_sql
}

pub fn get_inserts_from_csv(
    folder_name: &str,
    name: &str,
    schema: &Schema,
) -> Result<Vec<String>, Box<dyn Error>> {
    let mut rdr = read_csv(folder_name, name)?;

    let mut sql_list = vec![];
    for record in rdr.records() {
        let record = record?;
        let mut values = vec![];
        for (idx, f) in schema.fields.iter().enumerate() {
            let val = record.get(idx).unwrap();
            let val = match f.typ {
                FieldType::Int | FieldType::UInt | FieldType::Float | FieldType::Decimal => {
                    if val.is_empty() {
                        "0".to_string()
                    } else {
                        val.to_string()
                    }
                }
                FieldType::String | FieldType::Text | FieldType::Timestamp => {
                    format!("'{val}'")
                }
                _ => {
                    format!("'{val}'")
                }
            };
            values.push(val);
        }
        let columns: Vec<String> = schema
            .fields
            .iter()
            .map(|f| {
                f.name
                    .clone()
                    .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
            })
            .collect();
        sql_list.push(format!(
            "INSERT INTO {}({}) values ({})",
            name,
            columns.join(","),
            values.join(",")
        ));
    }
    Ok(sql_list)
}

pub fn query_sqlite(
    mapper: Arc<Mutex<SqlMapper>>,
    sql: &str,
    schema: &Schema,
) -> Result<Vec<Record>, rusqlite::Error> {
    mapper
        .lock()
        .map(|mapper_guard| -> rusqlite::Result<Vec<Record>> {
            let mut stmt = mapper_guard.conn.prepare(sql)?;
            let mut rows = stmt.query(())?;

            let mut records = vec![];

            while let Ok(Some(row)) = rows.next() {
                let record = map_sqlite_to_record(schema, row)?;
                records.push(record);
            }
            Ok(records)
        })
        .unwrap()
}

pub fn map_sqlite_to_record(
    schema: &Schema,
    row: &rusqlite::Row,
) -> Result<Record, rusqlite::Error> {
    let mut values = vec![];

    for (idx, f) in schema.fields.clone().into_iter().enumerate() {
        let val = match_type! {
            f.typ,
            FieldType::UInt => convert_type!(Field::UInt, f, row, idx),
            FieldType::Int => convert_type!(Field::Int, f, row, idx),
            FieldType::Float => Field::Float(dozer_types::ordered_float::OrderedFloat(row.get(idx)?)),
            FieldType::Boolean => convert_type!(Field::Boolean, f, row, idx),
            FieldType::String => convert_type!(Field::String, f, row, idx),
            FieldType::Text => convert_type!(Field::Text, f, row, idx),
            FieldType::Binary => convert_type!(Field::Binary, f, row, idx),
            FieldType::Timestamp => convert_type!(Field::String, f, row, idx),
            FieldType::Decimal => {
                let val: String = row.get(idx)?;
                Field::Decimal(Decimal::from_str(&val).expect("decimal parse error"))
            },
            FieldType::Date =>  convert_type!(Field::String, f, row, idx),
            FieldType::U128 | FieldType::I128 | FieldType::Bson | FieldType::Point | FieldType::Duration => {
                panic!("type not supported : {:?}", f.typ.to_owned())
            }
        };
        values.push(val);
    }
    let record = Record {
        schema_id: schema.identifier,
        values,
        version: None,
    };
    Ok(record)
}

fn parse_sql_number(n: &str) -> Field {
    match n.parse::<i64>() {
        Ok(n) => Field::Int(n),
        Err(_) => Field::Float(OrderedFloat(n.parse::<f64>().unwrap())),
    }
}
pub fn parse_exp_to_string(exp: &Expr) -> String {
    if let sqlparser::ast::Expr::Value(value) = exp {
        value.to_string()
    } else {
        panic!("not supported");
    }
}

pub fn parse_exp_to_field(exp: &Expr) -> Field {
    match &exp {
        Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(str, _) => parse_sql_number(str),
            sqlparser::ast::Value::SingleQuotedString(str) => Field::String(str.to_owned()),
            sqlparser::ast::Value::Boolean(b) => Field::Boolean(*b),
            sqlparser::ast::Value::Null => Field::Null,
            _ => {
                panic!("{}", format!("{value} not supported"))
            }
        },
        _ => panic!("{}", format!("{exp} not supported")),
    }
}
pub fn get_primary_key_value(schema: &Schema, rec: &Record) -> Field {
    let idx = schema.primary_index[0];

    rec.get_value(idx)
        .expect("field with idx is missing")
        .to_owned()
}

pub fn get_primary_key_name(schema: &Schema) -> String {
    let idx = schema.primary_index[0];
    schema.fields[idx]
        .name
        .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
}

pub fn get_primary_key_index(schema: &Schema) -> usize {
    schema.primary_index[0]
}

pub fn get_table_name(name: &ObjectName) -> String {
    name.0[0].value.clone()
}

pub fn map_field_to_string(f: &Field) -> String {
    match f {
        Field::UInt(i) => i.to_string(),
        Field::U128(i) => i.to_string(),
        Field::Int(i) => i.to_string(),
        Field::I128(i) => i.to_string(),
        Field::Float(i) => i.to_string(),
        Field::Boolean(i) => i.to_string(),
        Field::String(i) => format!("'{i}'"),
        Field::Text(i) => i.to_string(),
        Field::Timestamp(i) => i.to_string(),
        Field::Date(i) => i.to_string(),
        Field::Binary(i) => str::from_utf8(i).unwrap().to_string(),
        Field::Bson(_) => panic!("not supported {f:?}"),
        Field::Decimal(i) => i.to_string(),
        Field::Point(p) => format!("'{:?}'", p.0.x_y()),
        Field::Duration(d) => d.to_string(),
        Field::Null => "null".to_string(),
    }
}

pub fn get_schema(columns: &[rusqlite::Column]) -> Schema {
    Schema {
        identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
        fields: columns
            .iter()
            .map(|c| {
                let typ = c
                    .decl_type()
                    .map_or("string".to_string(), |a| a.to_ascii_lowercase());

                FieldDefinition {
                    name: c
                        .name()
                        .to_string()
                        .replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
                    typ: match typ.as_str() {
                        "integer" => FieldType::Int,
                        "string" | "text" => FieldType::String,
                        "real" => FieldType::Float,
                        "numeric" => FieldType::Decimal,
                        "timestamp" => FieldType::Timestamp,
                        f => panic!("unknown field_type : {f}"),
                    },
                    nullable: true,
                    source: SourceDefinition::Dynamic,
                }
            })
            .collect(),
        primary_index: vec![0],
    }
}
