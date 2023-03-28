mod arg;
mod error;
mod helper;
mod validator;

use arg::SqlLogicTestArgs;
use clap::Parser;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::parser::Parser as SqlParser;
use dozer_types::types::Operation;
use error::DozerSqlLogicTestError;
use error::Result;
use helper::helper::get_table_name;
use helper::mapper::SqlMapper;
use helper::pipeline::TestPipeline;
use rusqlite::types::Type;
use sqllogictest::{default_validator, parse_file, update_test_file, AsyncDB, DBOutput, Runner};
use sqlparser::dialect::AnsiDialect;
use validator::Validator;
use walkdir::WalkDir;

pub struct Dozer {
    // `source_db` is used to execute create table sql.
    // Then collect schema in tables.
    pub source_db: SqlMapper,
    // Key is table name, value is operation(Insert/Update/Delete) for the table.
    pub ops: Vec<(String, Operation)>,
}

impl Dozer {
    pub fn create(source_db: SqlMapper) -> Self {
        Self {
            source_db,
            ops: vec![],
        }
    }

    // Only in dozer and sink results to **results** table
    pub fn run_pipeline(&mut self, sql: &str) -> Result<()> {
        let pipeline = TestPipeline::new(
            sql.to_string(),
            self.source_db.schema_map.clone(),
            self.ops.clone(),
        );
        pipeline.run()?;
        Ok(())
    }

    // Run `select * from results` to check results
    pub fn check_results(&mut self) -> Result<DBOutput> {
        let mut stmt = self.source_db.conn.prepare("select * from Change_Log")?;
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
        Ok(DBOutput::Rows {
            types: vec![],
            rows: parsed_rows,
        })
    }
}

#[async_trait::async_trait]
impl AsyncDB for Dozer {
    type Error = DozerSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        use std::println as info;
        info!("SQL [{}] is running", sql);

        let ast = SqlParser::parse_sql(&AnsiDialect {}, sql)?;
        let statement: &Statement = &ast[0];
        match statement {
            Statement::Query(_) => {
                let change_log = self.source_db.get_change_log().unwrap();

                self.ops = vec![];
                for change_operation in change_log {
                    let (source, operation) = self.source_db.get_operation(change_operation);
                    self.ops.push((source, operation));
                }

                self.run_pipeline(sql)?;

                // drop table results
                // let dest_db = self.dest_db.lock().unwrap();
                // dest_db.conn.execute("drop table results", ())?;
                self.check_results()
            }
            // If statement is Insert/Update/Delete, collect ops from sql
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                self.source_db.conn.execute(sql, ())?;
                return Ok(DBOutput::StatementComplete(0));
            }
            // If sql is create table, run `source_db` to get table schema
            Statement::CreateTable { name, .. } => {
                let table_name = get_table_name(name);
                // create table and get schema
                self.source_db.create_table(&table_name, sql)?;
                return Ok(DBOutput::StatementComplete(0));
            }
            _ => panic!("{}", format!("{statement} is not supported")),
        }
    }

    fn engine_name(&self) -> &str {
        "Dozer"
    }
}

fn create_dozer() -> Result<Dozer> {
    // create dozer
    let source_db = SqlMapper::default();
    let dozer = Dozer::create(source_db);
    Ok(dozer)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = SqlLogicTestArgs::parse();
    //let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir("dozer-tests/src/sqllogictest/test_suits").unwrap();
    let mut files = vec![];
    for suit in suits {
        let suit = suit.unwrap().path();
        for entry in WalkDir::new(suit)
            .min_depth(0)
            .max_depth(100)
            .sort_by(|a, b| a.file_name().cmp(b.file_name()))
            .into_iter()
            .filter(|e| !e.as_ref().unwrap().file_type().is_dir())
        {
            files.push(entry)
        }
    }
    for file in files.into_iter() {
        let file_path = file.as_ref().unwrap().path();
        let mut runner = Runner::new(create_dozer()?);
        let records = parse_file(file_path).unwrap();
        if args.complete {
            // Use validator db to generate expected results
            let mut validator_runner = Runner::new(Validator::create());
            let col_separator = " ";
            let validator = default_validator;
            update_test_file(
                file.unwrap().path(),
                &mut validator_runner,
                col_separator,
                validator,
            )
            .await
            .unwrap();
            // Run dozer to check if dozer's outputs satisfy expected results
            for record in records.iter() {
                runner.run_async(record.clone()).await?;
            }
        } else {
            for record in records.iter() {
                runner.run_async(record.clone()).await?;
            }
        }
    }
    Ok(())
}
