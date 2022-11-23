use dozer_types::errors::types;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SchemaIdentifier};
use sqlparser::ast::{Expr, ObjectName};
use std::error::Error;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use super::SqlMapper;

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
                FieldType::Float => "numeric",
                FieldType::Timestamp => "timestamp",
                FieldType::Boolean => "bool",

                typ => panic!("unsupported type {:?}", typ),
            };
            format!(
                "{} {}",
                f.name.replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
                typ
            )
        })
        .collect::<Vec<String>>()
        .join(",");
    let creation_sql = format!("CREATE TABLE {} ({})", name, columns);
    println!("CREATION: {:?}", creation_sql);
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
                FieldType::String | FieldType::Text | FieldType::Timestamp | _ => {
                    format!("'{}'", val.replace("'", "\'"))
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

pub fn query_sqllite(
    mapper: Arc<Mutex<SqlMapper>>,
    sql: &str,
) -> Result<Vec<Record>, rusqlite::Error> {
    let schema = mapper
        .lock()
        .map(|mapper_guard| -> rusqlite::Result<Schema> {
            let stmt = mapper_guard.conn.prepare(sql)?;
            let columns = stmt.columns();
            let schema = get_schema(&columns);
            Ok(schema)
        })
        .unwrap()
        .unwrap();
    mapper
        .lock()
        .map(|mapper_guard| -> rusqlite::Result<Vec<Record>> {
            let mut stmt = mapper_guard.conn.prepare(sql)?;
            let mut rows = stmt.query(())?;

            let mut records = vec![];

            while let Ok(Some(row)) = rows.next() {
                let mut values = vec![];
                let mut idx = 0;
                for f in schema.fields.clone() {
                    values.push(match f.typ {
                        dozer_types::types::FieldType::UInt => Field::UInt(row.get(idx)?),
                        dozer_types::types::FieldType::Int => Field::Int(row.get(idx)?),
                        dozer_types::types::FieldType::Float => {
                            Field::Float(dozer_types::ordered_float::OrderedFloat(row.get(idx)?))
                        }
                        dozer_types::types::FieldType::Boolean => Field::Boolean(row.get(idx)?),
                        dozer_types::types::FieldType::String => Field::String(row.get(idx)?),
                        dozer_types::types::FieldType::Text => Field::Text(row.get(idx)?),
                        dozer_types::types::FieldType::Binary => Field::Binary(row.get(idx)?),
                        dozer_types::types::FieldType::Timestamp => {
                            let val: String = row.get(idx)?;
                            Field::String(val)
                        }
                        dozer_types::types::FieldType::Decimal => {
                            let val: String = row.get(idx)?;
                            Field::Decimal(Decimal::from_str(&val).expect("decimal parse error"))
                        }

                        dozer_types::types::FieldType::Null => Field::Null,
                        dozer_types::types::FieldType::UIntArray
                        | dozer_types::types::FieldType::IntArray
                        | dozer_types::types::FieldType::FloatArray
                        | dozer_types::types::FieldType::BooleanArray
                        | dozer_types::types::FieldType::StringArray
                        | dozer_types::types::FieldType::Bson => {
                            panic!("type not supported : {:?}", f.typ.to_owned())
                        }
                    });
                    idx += 1;
                }
                let record = Record {
                    schema_id: schema.identifier.clone(),
                    values,
                };
                records.push(record);
            }
            Ok(records)
        })
        .unwrap()
}

pub fn parse_sql_number(n: &str) -> Result<Field, types::TypeError> {
    match n.parse::<i64>() {
        Ok(n) => Ok(Field::Int(n)),
        Err(_) => match n.parse::<f64>() {
            Ok(f) => Ok(Field::Float(OrderedFloat(f))),
            Err(_) => Err(types::TypeError::InvalidFieldValue(n.to_string())),
        },
    }
}

pub fn parse_exp_to_field(exp: &Expr) -> Field {
    match &exp {
        sqlparser::ast::Expr::Value(value) => match value {
            sqlparser::ast::Value::Number(str, _) => parse_sql_number(str).unwrap(),
            sqlparser::ast::Value::SingleQuotedString(str) => Field::String(str.to_owned()),
            sqlparser::ast::Value::Boolean(b) => Field::Boolean(*b),
            sqlparser::ast::Value::Null => Field::Null,
            _ => {
                panic!("not supported")
            }
        },
        _ => panic!("not supported"),
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
        .to_owned()
}

pub fn get_table_name(name: &ObjectName) -> String {
    name.0[0].value.clone()
}

pub fn map_field_to_string(f: &Field) -> String {
    match f {
        Field::UInt(i) => i.to_string(),
        Field::Int(i) => i.to_string(),
        Field::Float(i) => i.to_string(),
        Field::Boolean(i) => i.to_string(),
        Field::String(i) => format!("'{}'", i.to_string()),
        Field::Text(i) => i.to_string(),
        Field::Binary(_)
        | Field::UIntArray(_)
        | Field::IntArray(_)
        | Field::FloatArray(_)
        | Field::BooleanArray(_)
        | Field::StringArray(_)
        | Field::Timestamp(_)
        | Field::Bson(_) => panic!("not supported {:?}", f),
        Field::Decimal(i) => i.to_string(),
        Field::Null => "null".to_string(),
    }
}

pub fn get_schema(columns: &Vec<rusqlite::Column>) -> Schema {
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
                        "string" => FieldType::String,
                        "text" => FieldType::Text,
                        "numeric" => FieldType::Float,
                        "timestamp" => FieldType::Timestamp,
                        f => panic!("unknown field_type : {}", f),
                    },
                    nullable: true,
                }
            })
            .collect(),
        values: vec![],
        primary_index: vec![0],
        secondary_indexes: vec![],
    }
}

pub fn read_csv(folder_name: &str, name: &str) -> Result<csv::Reader<std::fs::File>, csv::Error> {
    let current_dir = std::env::current_dir().unwrap();
    println!("{:?}", current_dir);
    let paths = vec![
        format!("../target/debug/{}-data/{}.csv", folder_name, name),
        format!("./target/debug/{}-data/{}.csv", folder_name, name),
    ];

    let mut err = None;
    for path in paths {
        let rdr = csv::Reader::from_path(Path::new(path.as_str()));
        match rdr {
            Ok(rdr) => return Ok(rdr),
            Err(e) => err = Some(Err(e)),
        }
    }
    err.unwrap()
}

pub fn download(folder_name: &str) {
    let path = format!("../target/debug/{}", folder_name);
    let exists = Path::new(&path).is_dir();
    if !exists {
        Command::new("sh")
            .arg("-C")
            .arg(format!("./scripts/download_{}.sh", folder_name))
            .spawn()
            .expect("sh command failed to start");
    }
}
