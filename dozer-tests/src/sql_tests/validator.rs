use crate::error::DozerSqlLogicTestError;
use rusqlite::types::Type;
use sqllogictest::{AsyncDB, DBOutput};

// Used in `complete` mode, to generate results
pub struct Validator {
    pub conn: rusqlite::Connection,
}

impl Validator {
    pub fn create() -> Self {
        Self {
            conn: rusqlite::Connection::open_in_memory().unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncDB for Validator {
    type Error = DozerSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput, Self::Error> {
        let sql = sql.trim_start();
        if sql.to_lowercase().starts_with("select") || sql.to_lowercase().starts_with("with") {
            let mut stmt = self.conn.prepare(sql)?;
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
                            parsed_row.push(
                                String::from_utf8(val_ref.as_blob().unwrap().to_vec()).unwrap(),
                            );
                        }
                    }
                }
                parsed_rows.push(parsed_row);
            }
            Ok(DBOutput::Rows {
                types: vec![],
                rows: parsed_rows,
            })
        } else {
            self.conn.execute(sql, ())?;
            Ok(DBOutput::StatementComplete(0))
        }
    }

    fn engine_name(&self) -> &str {
        "validator"
    }
}
