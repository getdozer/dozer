use std::collections::HashMap;

use super::schema::*;
use crate::error::Result;
use dozer_types::chrono::{DateTime, NaiveDateTime, Offset, Utc};
use dozer_types::json_types::JsonValue;
use dozer_types::json_value_to_field;
use dozer_types::rust_decimal::prelude::FromPrimitive;
use dozer_types::rust_decimal::Decimal;
use dozer_types::serde_json;
use dozer_types::types::{Field, FieldType, Operation, Record, Schema};
use rusqlite::config::DbConfig;
use rusqlite::types::Type;

#[derive(Debug)]
pub struct SqlMapper {
    // Key is table name, value is schema in the table
    // Schema is dozer inner-defined
    pub schema_map: HashMap<String, Schema>,
    // Used by execute create table sql.
    pub conn: rusqlite::Connection,
}

impl Default for SqlMapper {
    fn default() -> Self {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        //let conn = rusqlite::Connection::open("mydb.sqlite").unwrap();
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, true)
            .expect("Unable to enable triggers");

        let sql_trigger = r#"CREATE TABLE Change_Log (
            Log_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Change_Time DATETIME DEFAULT CURRENT_TIMESTAMP,
            Change_Type CHAR(1),
            Table_Name TEXT,
            Record_ID INTEGER,
            Old_Value TEXT,
            New_Value TEXT
        );"#
        .to_string();
        conn.execute(&sql_trigger, ()).unwrap();

        Self {
            schema_map: Default::default(),
            conn,
        }
    }
}

impl SqlMapper {
    pub fn insert_schema(&mut self, name: String, schema: Schema) {
        self.schema_map.insert(name, schema);
    }

    pub fn get_change_log(&mut self) -> Result<Vec<Vec<String>>> {
        let mut stmt = self.conn.prepare("select * from Change_Log")?;
        let column_count = stmt.column_count();
        let mut rows = stmt.query(())?;
        let mut parsed_rows = vec![];
        while let Ok(Some(row)) = rows.next() {
            let mut parsed_row = vec![];
            for idx in 0..column_count {
                let val_ref = row.get_ref::<usize>(idx)?;
                match val_ref.data_type() {
                    Type::Null => {
                        parsed_row.push("NULL".to_string());
                    }
                    Type::Integer => {
                        parsed_row.push(val_ref.as_i64().unwrap().to_string());
                    }
                    Type::Real => {
                        parsed_row.push(val_ref.as_f64().unwrap().to_string());
                    }
                    Type::Text => {
                        parsed_row.push(val_ref.as_str().unwrap().to_string());
                    }
                    Type::Blob => {
                        parsed_row
                            .push(String::from_utf8(val_ref.as_blob().unwrap().to_vec()).unwrap());
                    }
                }
            }
            parsed_rows.push(parsed_row);
        }
        // self.conn
        //     .execute("DELETE FROM Change_Log", ())
        //     .expect("Unable to clear the change log");
        Ok(parsed_rows)
    }

    pub fn get_operation(&self, change_operation: Vec<String>) -> (String, Operation) {
        let operation = change_operation[2].clone();
        let table_name = change_operation[3].clone();
        let old_data = change_operation[5].clone();
        let new_data = change_operation[6].clone();

        let schema = self.schema_map.get(&table_name).unwrap();

        let operation = match operation.as_str() {
            "I" => get_insert(new_data, schema),
            "U" => get_update(old_data, new_data, schema),
            "D" => get_delete(old_data, schema),
            _ => panic!("Operation is not supported"),
        };

        (table_name, operation)
    }

    pub fn get_schema_from_conn(&self, table_name: &str) -> Result<Schema> {
        let stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_name} LIMIT 1"))?;

        let columns = stmt.columns();
        Ok(get_schema(&columns))
    }

    pub fn create_table(&mut self, table_name: &str, table_sql: &str) -> Result<()> {
        self.conn.execute(table_sql, ())?;
        let schema = { self.get_schema_from_conn(table_name).unwrap() };
        self.insert_schema(table_name.to_string(), schema.clone());

        let json_conversion: String = get_json_object_conversion("NEW", &schema);
        let insert_trigger = format!(
            "CREATE TRIGGER {table_name}_insert_trigger \
                AFTER INSERT ON {table_name} \
                FOR EACH ROW \
                BEGIN \
                    INSERT INTO Change_Log (Change_Type, Table_Name, Record_ID, New_Value) \
                    VALUES ('I', '{table_name}', NEW.rowid, {json_conversion}); \
                END;"
        );

        let json_conversion: String = get_json_object_conversion("OLD", &schema);
        let delete_trigger = format!(
            "CREATE TRIGGER {table_name}_delete_trigger \
                AFTER DELETE ON {table_name} \
                FOR EACH ROW \
                BEGIN \
                    INSERT INTO Change_Log (Change_Type, Table_Name, Record_ID, Old_Value) \
                    VALUES ('D', '{table_name}', OLD.rowid, {json_conversion}); \
                END;"
        );

        let old_json_conversion: String = get_json_object_conversion("OLD", &schema);
        let new_json_conversion: String = get_json_object_conversion("NEW", &schema);
        let update_trigger = format!(
                "CREATE TRIGGER {table_name}_update_Trigger \
                AFTER UPDATE ON {table_name} \
                FOR EACH ROW \
                BEGIN \
                    INSERT INTO Change_Log (Change_Type, Table_Name, Record_ID, Old_Value, New_Value) \
                    VALUES ('U', '{table_name}', OLD.rowid, {old_json_conversion}, {new_json_conversion}); \
                END;"
                );

        self.conn
            .execute(&insert_trigger, ())
            .expect("unable to create trigger");
        self.conn
            .execute(&delete_trigger, ())
            .expect("unable to create trigger");
        self.conn
            .execute(&update_trigger, ())
            .expect("unable to create trigger");

        Ok(())
    }
}

fn get_json_object_conversion(prefix: &str, schema: &Schema) -> String {
    let mut fields = vec![];
    for field in schema.fields.iter() {
        let field_name = field
            .name
            .replace(|c: char| !c.is_ascii_alphanumeric(), "_");
        let full_name = format!("'{field_name}', {prefix}.{field_name}");
        fields.push(full_name);
    }
    let fields_text = fields.join(", ");
    format!("json_object({fields_text})")
}

fn get_delete(data: String, schema: &Schema) -> Operation {
    let old = get_record_from_json(data, schema);
    Operation::Delete { old }
}

fn get_insert(data: String, schema: &Schema) -> Operation {
    let new = get_record_from_json(data, schema);
    Operation::Insert { new }
}

fn get_update(old_data: String, new_data: String, schema: &Schema) -> Operation {
    let old = get_record_from_json(old_data, schema);
    let new = get_record_from_json(new_data, schema);
    Operation::Update { old, new }
}

fn get_record_from_json(data: String, schema: &Schema) -> Record {
    let root: JsonValue = serde_json::from_str(&data).unwrap();
    let mut record = Record {
        values: vec![],
        lifetime: None,
    };

    for field_definition in schema.fields.iter() {
        let field_type = field_definition.typ;
        let field_name = field_definition.name.as_str();
        let json_value = root.get(field_name).unwrap();

        let value = match field_definition.typ {
            FieldType::UInt
            | FieldType::Int
            | FieldType::Float
            | FieldType::Boolean
            | FieldType::String
            | FieldType::Text => json_value_to_field(
                json_value.clone(),
                field_definition.typ,
                field_definition.nullable,
            )
            .unwrap(),
            FieldType::Decimal => {
                Field::Decimal(Decimal::from_f64(json_value.to_f64().unwrap()).unwrap())
            }
            FieldType::Timestamp => {
                let naive_date_time = NaiveDateTime::parse_from_str(
                    json_value.as_string().unwrap(),
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap();

                Field::Timestamp(DateTime::from_naive_utc_and_offset(
                    naive_date_time,
                    Utc.fix(),
                ))
            }
            _ => panic!("Unsupported field type: {field_type:?}"),
        };

        record.values.push(value);
    }

    record
}
