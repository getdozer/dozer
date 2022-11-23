use std::collections::HashMap;

use super::helper::*;
use dozer_types::types::{Field, Operation, Record, Schema};
use sqlparser::ast::{BinaryOperator, Expr, Statement};
use sqlparser::ast::{SetExpr, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::*;
pub struct SchemaResponse {
    schema: Schema,
    name: String,
    key: Vec<u8>,
}

pub struct SqlMapper {
    pub schema_map: HashMap<String, Schema>,
    pub record_map: HashMap<(String, Vec<u8>), Record>,
    pub conn: rusqlite::Connection,
}

impl Default for SqlMapper {
    fn default() -> Self {
        Self {
            schema_map: Default::default(),
            record_map: Default::default(),
            conn: rusqlite::Connection::open_in_memory().unwrap(),
        }
    }
}

impl SqlMapper {
    pub fn insert_schema(&mut self, name: String, schema: Schema) {
        self.schema_map.insert(name, schema);
    }

    pub fn get_schema_from_conn(&self, table_name: &str) -> rusqlite::Result<Schema> {
        let stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {} LIMIT 1", table_name))?;

        let columns = stmt.columns();
        Ok(get_schema(&columns))
    }

    pub fn execute_list(
        &mut self,
        list: Vec<(String, String)>,
    ) -> rusqlite::Result<Vec<Operation>> {
        let mut ops = vec![];
        for (_schema_name, sql) in list {
            self.conn.execute(sql.as_str(), ())?;
            let op = self.get_operation_from_sql(&sql);
            ops.push(op);
        }
        Ok(ops)
    }

    pub fn get_schema(&self, name: String) -> &Schema {
        self.schema_map
            .get(&name)
            .expect(&format!("Schema is missing: {}", name))
    }

    pub fn get_operation_from_sql(&mut self, sql: &str) -> Operation {
        let dialect = GenericDialect {};

        let ast = Parser::parse_sql(&dialect, sql).unwrap();

        let st: &Statement = &ast[0];
        match st {
            Statement::Insert {
                or: _,
                into: _,
                columns: _,
                overwrite: _,
                partitioned: _,
                table_name,
                source,

                after_columns: _,
                table: _,
                on: _,
            } => {
                let values: Vec<Field> = match &*source.body {
                    SetExpr::Values(values) => values.0[0].iter().map(parse_exp_to_field).collect(),
                    _ => panic!("not supported"),
                };
                let name = get_table_name(table_name);
                let schema = self
                    .schema_map
                    .get(&name)
                    .expect("schema is not inserted with the name");
                let rec = Record::new(schema.identifier.clone(), values);
                let key = get_primary_key_value(schema, &rec);
                self.record_map
                    .insert((name, key.to_bytes().unwrap()), rec.clone());
                Operation::Insert { new: rec }
            }

            Statement::Update {
                table,
                assignments,
                from: _,
                selection,
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
                        .expect(&format!("field not found with name : {:?}", id.value))
                        .to_owned();

                    rec2.values[idx.0] = parse_exp_to_field(&a.value);
                }
                self.record_map
                    .insert((schema_res.name, schema_res.key), rec2.clone());

                Operation::Update {
                    old: rec.clone(),
                    new: rec2,
                }
            }
            Statement::Delete {
                table_name,
                using: _,
                selection,
            } => {
                let (rec, _) = self.map_selection(&table_name, selection);
                Operation::Delete { old: rec }
            }
            _ => panic!("Not supported"),
        }
    }

    pub fn create_tables(&mut self, tables: Vec<(&str, &str)>) -> rusqlite::Result<()> {
        for (table_name, table_sql) in tables {
            self.conn.execute(table_sql, ())?;
            let schema = { self.get_schema_from_conn(table_name).unwrap() };
            self.insert_schema(table_name.to_string(), schema);
        }
        Ok(())
    }

    pub fn map_operation_to_sql(&self, name: &String, op: Operation) -> String {
        let schema = self
            .schema_map
            .get(name)
            .expect(&format!("Schema is missing: {}", name));
        let pkey_name = get_primary_key_name(schema);

        match op {
            Operation::Delete { old } => {
                let pkey_value = get_primary_key_value(schema, &old);
                format!(
                    "DELETE FROM {} WHERE {}={}",
                    name,
                    pkey_name,
                    map_field_to_string(&pkey_value)
                )
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
                format!(
                    "INSERT INTO {}({}) values ({})",
                    name, column_names, values_str
                )
            }
            Operation::Update { old, new } => {
                let pkey_value = get_primary_key_value(schema, &old);
                let values_str = new
                    .values
                    .iter()
                    .enumerate()
                    .filter(|(idx, a)| old.values[*idx] != **a)
                    .map(|(idx, a)| {
                        format!(
                            "{}={}",
                            schema.fields[idx]
                                .name
                                .replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
                            map_field_to_string(a)
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(",");
                format!(
                    "UPDATE {} SET {} WHERE {}={}",
                    name,
                    values_str,
                    pkey_name,
                    map_field_to_string(&pkey_value)
                )
            }
        }
    }

    fn map_selection(
        &self,
        table_factor: &TableFactor,
        selection: &Option<Expr>,
    ) -> (Record, SchemaResponse) {
        if let TableFactor::Table {
            name,
            alias: _,
            args: _,
            with_hints: _,
        } = table_factor
        {
            let name = get_table_name(name);
            let schema = self
                .schema_map
                .get(&name)
                .expect(&format!("Schema is missing: {}", name))
                .to_owned();

            if let Some(Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            }) = selection
            {
                let column_name = match *left.to_owned() {
                    Expr::Identifier(ident) => ident.value,
                    _ => panic!("not supported: {:?}", left),
                };

                let val = parse_exp_to_field(&*right.to_owned()).to_bytes().unwrap();

                assert_eq!(
                    column_name,
                    get_primary_key_name(&schema),
                    "Updates only on primary key supported"
                );

                let rec = self
                    .record_map
                    .get(&(name.clone(), val.clone()))
                    .expect("record is to be inserted before update/delete");
                (
                    rec.to_owned(),
                    SchemaResponse {
                        schema,
                        name,
                        key: val,
                    },
                )
            } else {
                panic!("not supported: {:?}", selection);
            }
        } else {
            panic!("not supported: {:?}", selection);
        }
    }
}
