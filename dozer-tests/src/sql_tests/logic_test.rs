mod arg;
mod error;
mod helper;
mod validator;

use std::path::Path;

use arg::SqlLogicTestArgs;
use clap::Parser;
// use arg::SqlLogicTestArgs;
// use clap::Parser;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::DozerDialect;
use dozer_sql::sqlparser::parser::Parser as SqlParser;
use dozer_types::types::Operation;
use error::DozerSqlLogicTestError;
use error::Result;
use helper::mapper::SqlMapper;
use helper::pipeline::TestPipeline;
use helper::schema::get_table_name;

use libtest_mimic::Trial;
use sqllogictest::default_column_validator;
use sqllogictest::harness;
use sqllogictest::DefaultColumnType;
use sqllogictest::{default_validator, AsyncDB, DBOutput, Runner};
use tokio::runtime::Runtime;
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
    pub async fn run_pipeline(&mut self, sql: &str) -> Result<Vec<Vec<String>>> {
        let pipeline = TestPipeline::new(
            sql.to_string(),
            self.source_db.schema_map.clone(),
            self.ops.clone(),
        );
        pipeline.run().await.map_err(DozerSqlLogicTestError::from)
    }
}

#[async_trait::async_trait]
impl AsyncDB for Dozer {
    type Error = DozerSqlLogicTestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        let ast = SqlParser::parse_sql(&DozerDialect {}, sql)?;
        let statement: &Statement = &ast[0];
        match statement {
            // If sql is create table, run `source_db` to get table schema
            Statement::CreateTable { name, .. } => {
                let table_name = get_table_name(name);
                // create table and get schema
                self.source_db.create_table(&table_name, sql)?;
                return Ok(DBOutput::StatementComplete(0));
            }
            // If statement is Insert/Update/Delete, collect ops from sql
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                self.source_db.conn.execute(sql, ())?;
                return Ok(DBOutput::StatementComplete(0));
            }
            Statement::Query(_) => {
                let change_log = self.source_db.get_change_log().unwrap();

                self.ops = vec![];
                for change_operation in change_log {
                    let (source, operation) = self.source_db.get_operation(change_operation);
                    self.ops.push((source, operation));
                }

                let output = self.run_pipeline(sql).await?;

                Ok(DBOutput::Rows {
                    types: vec![],
                    rows: output,
                })

                // drop table results
                // let dest_db = self.dest_db.lock().unwrap();
                // dest_db.conn.execute("drop table results", ())?;
                //self.check_results()
            }

            _ => panic!("{}", format!("{statement} is not supported")),
        }
    }

    fn engine_name(&self) -> &str {
        "Dozer"
    }
}

async fn create_dozer() -> Result<Dozer> {
    // create dozer
    let source_db = SqlMapper::default();
    let dozer = Dozer::create(source_db);
    Ok(dozer)
}

const BASE_PATH: &str = "src/sql_tests";

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = SqlLogicTestArgs::parse();
    //let suits = SqlLogicTestArgs::parse().suites;

    let complete = args.complete;

    let current_suits = std::fs::read_dir(format!("{BASE_PATH}/full")).unwrap();
    if complete {
        delete_dir_contents(current_suits);
        copy_dir_all(
            format!("{BASE_PATH}/prototype"),
            format!("{BASE_PATH}/full"),
        )?;
    }

    let suits = std::fs::read_dir(format!("{BASE_PATH}/full")).unwrap();
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

    let mut tests = vec![];
    for file in files.into_iter() {
        let file_path = file.unwrap().path().to_owned();

        if complete {
            // Use validator db to generate expected results
            let mut validator_runner = Runner::new(Validator::create);
            let col_separator = " ";
            let validator = default_validator;
            validator_runner
                .update_test_file(
                    &file_path,
                    col_separator,
                    validator,
                    default_column_validator,
                )
                .await
                .unwrap();
        }

        tests.push(Trial::test(
            file_path.to_str().unwrap().to_owned(),
            move || {
                let mut tester = Runner::new(create_dozer);
                let runtime = Runtime::new().unwrap();
                runtime.block_on(tester.run_file_async(file_path))?;
                Ok(())
            },
        ));
    }
    harness::run(&args.harness_args, tests).exit();
}

fn delete_dir_contents(dir: std::fs::ReadDir) {
    for entry in dir.flatten() {
        let path = entry.path();

        if path.is_dir() {
            std::fs::remove_dir_all(path).expect("Failed to remove a dir");
        } else {
            std::fs::remove_file(path).expect("Failed to remove a file");
        }
    }
}

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<()> {
    std::fs::create_dir_all(&dst).expect("Failed to create dir");
    for entry in std::fs::read_dir(src).expect("Failed to read dir") {
        let entry = entry.expect("Failed to read dir entry");
        let ty = entry.file_type().expect("Failed to get entry type");
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))
                .expect("Failed to copy file");
        }
    }
    Ok(())
}
