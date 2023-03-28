use std::collections::HashMap;

use super::helper::*;
use crate::error::Result;
use dozer_core::errors::ExecutionError;
use dozer_sql::sqlparser::ast::{BinaryOperator, Expr, Statement};
use dozer_sql::sqlparser::ast::{SetExpr, TableFactor};
use dozer_sql::sqlparser::parser::*;
use dozer_types::json_value_to_field;
use dozer_types::serde_json::{self, Value};
use dozer_types::types::{Field, Operation, Record, Schema};
use rusqlite::config::DbConfig;
use rusqlite::hooks;
use rusqlite::types::Type;
use sqlparser::dialect::AnsiDialect;

pub struct SchemaResponse {
    schema: Schema,
}

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
        let conn = rusqlite::Connection::open("mydb.sqlite").unwrap();
        conn.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, true)
            .expect("Unable to enable triggers");

        let triggers_enabled = conn
            .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
            .expect("Unable to get triggers status");

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
        self.schema_map.insert(name.clone(), schema);
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
        self.conn
            .execute("DELETE FROM Change_Log", ())
            .expect("Unable to clear the change log");
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

    pub fn execute_list(
        &mut self,
        list: Vec<(&'static str, String)>,
    ) -> Result<Vec<(String, Operation)>> {
        // set up the update hook
        self.conn.update_hook(Some(
            |action: hooks::Action, dbname: &str, table_name: &str, rowid: i64| {
                println!(
                    "update_hook: action={:?}, dbname={:?}, table_name={:?}, rowid={:?}",
                    action, dbname, table_name, rowid
                );
            },
        ));

        let mut ops = vec![];
        for (_, sql) in list {
            let op = self.get_operation_from_sql(&sql);
            self.conn.execute(sql.as_str(), ())?;
            ops.push(op);
        }
        Ok(ops)
    }

    pub fn get_schema_from_conn(&self, table_name: &str) -> Result<Schema> {
        let stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_name} LIMIT 1"))?;

        let columns = stmt.columns();
        Ok(get_schema(&columns))
    }

    pub fn get_operation_from_sql(&mut self, sql: &str) -> (String, Operation) {
        let ast = Parser::parse_sql(&AnsiDialect {}, sql).unwrap();
        let st: &Statement = &ast[0];
        match st {
            Statement::Insert {
                columns,
                table_name,
                source,
                ..
            } => {
                let expr_values: Vec<Field> = match &*source.body {
                    SetExpr::Values(values) => {
                        values.rows[0].iter().map(parse_exp_to_field).collect()
                    }
                    _ => panic!("{}", format!("{} not supported", &*source.body)),
                };
                let name = get_table_name(table_name);
                let schema = self
                    .schema_map
                    .get(&name)
                    .expect("schema is not inserted with the name");

                let mut values = vec![];
                for f in schema.fields.iter() {
                    let c_tuple = columns.iter().enumerate().find(|(_, c)| c.value == f.name);

                    match c_tuple {
                        Some((c_idx, _)) => {
                            let expr_value = expr_values.get(c_idx).expect("column value expected");
                            values.push(expr_value.to_owned());
                        }
                        None => {
                            values.push(Field::Null);
                        }
                    }
                }
                let rec = Record::new(schema.identifier, values, None);

                (name, Operation::Insert { new: rec })
            }

            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let (rec, schema_res) = self.map_selection(&table.relation, selection);
                let mut rec2 = rec.clone();

                for a in assignments {
                    let id = a.id[0].to_owned();
                    let idx = schema_res
                        .schema
                        .fields
                        .iter()
                        .enumerate()
                        .find(|(_, f)| {
                            f.name.replace(|c: char| !c.is_ascii_alphanumeric(), "_") == id.value
                        })
                        .unwrap_or_else(|| panic!("field not found with name : {:?}", id.value))
                        .to_owned();

                    rec2.values[idx.0] = parse_exp_to_field(&a.value);
                }
                (
                    "".to_string(),
                    Operation::Update {
                        old: rec,
                        new: rec2,
                    },
                )
            }
            Statement::Delete {
                table_name,
                selection,
                ..
            } => {
                let (rec, _) = self.map_selection(table_name, selection);
                ("".to_string(), Operation::Delete { old: rec })
            }
            _ => panic!("{}", format!("{st}Not supported")),
        }
    }

    pub fn create_table(&mut self, table_name: &str, table_sql: &str) -> Result<()> {
        self.conn.execute(table_sql, ())?;
        let schema = { self.get_schema_from_conn(table_name).unwrap() };
        self.insert_schema(table_name.to_string(), schema.clone());

        let json_conversion: String = get_json_object_converstion("NEW", &schema);
        let insert_trigger = format!(
            "CREATE TRIGGER {table_name}_insert_trigger \
                AFTER INSERT ON {table_name} \
                FOR EACH ROW \
                BEGIN \
                    INSERT INTO Change_Log (Change_Type, Table_Name, Record_ID, New_Value) \
                    VALUES ('I', '{table_name}', NEW.rowid, {json_conversion}); \
                END;"
        );

        let json_conversion: String = get_json_object_converstion("OLD", &schema);
        let delete_trigger = format!(
            "CREATE TRIGGER {table_name}_delete_trigger \
                AFTER DELETE ON {table_name} \
                FOR EACH ROW \
                BEGIN \
                    INSERT INTO Change_Log (Change_Type, Table_Name, Record_ID, Old_Value) \
                    VALUES ('D', '{table_name}', OLD.rowid, {json_conversion}); \
                END;"
        );

        let old_json_conversion: String = get_json_object_converstion("OLD", &schema);
        let new_json_conversion: String = get_json_object_converstion("NEW", &schema);
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

    pub fn map_operation_to_sql(&self, name: &String, op: Operation) -> Result<String> {
        let schema = self
            .schema_map
            .get(name)
            .unwrap_or_else(|| panic!("Schema is missing: {name}"));
        let pkey_name = get_primary_key_name(schema);

        match op {
            Operation::Delete { old } => {
                let pkey_value = get_primary_key_value(schema, &old);
                Ok(format!(
                    "DELETE FROM {} WHERE {}={};",
                    name,
                    pkey_name,
                    map_field_to_string(&pkey_value)
                ))
            }
            Operation::Insert { new } => {
                let column_names = schema
                    .fields
                    .iter()
                    .map(|f| {
                        f.name
                            .to_owned()
                            .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
                    })
                    .collect::<Vec<String>>()
                    .join(",");
                let values_str = new
                    .values
                    .iter()
                    .map(map_field_to_string)
                    .collect::<Vec<String>>()
                    .join(",");
                Ok(format!(
                    "INSERT INTO {name}({column_names}) values ({values_str});"
                ))
            }
            Operation::Update { old, new } => {
                let pkey_value = get_primary_key_value(schema, &old);

                let mut field_names = vec![];
                for (idx, v) in new.values.iter().enumerate() {
                    if idx != get_primary_key_index(schema) {
                        if old.values.get(idx).is_some() {
                            field_names.push(format!(
                                "{}={}",
                                schema.fields.get(idx).map_or(
                                    Err(ExecutionError::InternalStringError(
                                        "index out of bounds for schema".to_string()
                                    )),
                                    |f| Ok(f
                                        .name
                                        .replace(|c: char| !c.is_ascii_alphanumeric(), "_"))
                                )?,
                                map_field_to_string(v)
                            ))
                        }
                    }
                }
                let values_str = field_names.join(",");
                Ok(format!(
                    "UPDATE {} SET {} WHERE {}={};",
                    name,
                    values_str,
                    pkey_name,
                    map_field_to_string(&pkey_value)
                ))
            }
        }
    }

    fn map_selection(
        &self,
        table_factor: &TableFactor,
        selection: &Option<Expr>,
    ) -> (Record, SchemaResponse) {
        if let TableFactor::Table { name, .. } = table_factor {
            let name = get_table_name(name);
            let schema = self
                .schema_map
                .get(&name)
                .unwrap_or_else(|| panic!("Schema is missing: {name}"))
                .to_owned();

            if let Some(Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            }) = selection
            {
                let column_name = match *left.to_owned() {
                    Expr::Identifier(ident) => ident.value,
                    _ => panic!("not supported: {left:?}"),
                };

                let val = parse_exp_to_string(right);

                assert_eq!(
                    column_name,
                    get_primary_key_name(&schema),
                    "Updates only on primary key supported"
                );

                let rec = self
                    .get_record(&name, &column_name, &val, &schema)
                    .expect("record with id is expected");

                (rec, SchemaResponse { schema })
            } else {
                panic!("not supported: {selection:?}");
            }
        } else {
            panic!("not supported: {selection:?}");
        }
    }

    pub fn get_record(
        &self,
        table_name: &str,
        key_name: &str,
        val: &str,
        schema: &Schema,
    ) -> Result<Record> {
        let sql = format!("select * from {table_name} where {key_name} = {val};");
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query(())?;

        if let Some(row) = rows.next().unwrap() {
            // scan columns value
            map_sqlite_to_record(schema, row)
        } else {
            panic!("no rows found");
        }
    }
}

fn get_json_object_converstion(prefix: &str, schema: &Schema) -> String {
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
    let root: Value = serde_json::from_str(&data).unwrap();
    let mut record = Record {
        values: vec![],
        schema_id: None,
        version: None,
    };

    for field_definition in schema.fields.iter() {
        let field_name = &field_definition.name;

        let json_value = root.get(field_name).unwrap();

        let Ok(value) = json_value_to_field(json_value.clone(), field_definition.typ, field_definition.nullable) else {
            panic!("Cannot convert json value to field: {json_value:?}")
        };

        record.values.push(value);
    }

    record
}
