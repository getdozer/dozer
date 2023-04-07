use std::collections::HashMap;

use super::helper::*;
use crate::error::Result;
use dozer_core::errors::ExecutionError;
use dozer_sql::sqlparser::ast::{BinaryOperator, Expr, Statement};
use dozer_sql::sqlparser::ast::{SetExpr, TableFactor};
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::*;
use dozer_types::types::{Field, Operation, Record, Schema};

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
        Self {
            schema_map: Default::default(),
            conn: rusqlite::Connection::open_in_memory().unwrap(),
        }
    }
}

impl SqlMapper {
    pub fn insert_schema(&mut self, name: String, schema: Schema) {
        self.schema_map.insert(name, schema);
    }

    pub fn execute_list(
        &mut self,
        list: Vec<(&'static str, String)>,
    ) -> Result<Vec<(String, Operation)>> {
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

    pub fn get_schema(&self, name: &str) -> &Schema {
        self.schema_map
            .get(name)
            .unwrap_or_else(|| panic!("Schema is missing: {name}"))
    }

    pub fn get_operation_from_sql(&mut self, sql: &str) -> (String, Operation) {
        let dialect = GenericDialect {};

        let ast = Parser::parse_sql(&dialect, sql).unwrap();

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
                let rec = Record::new(schema.identifier, values);

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
        self.insert_schema(table_name.to_string(), schema);
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
                    if let Some(old_value) = old.values.get(idx) {
                        if old_value != v {
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
