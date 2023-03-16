use dozer_core::errors::ExecutionError;
use dozer_types::log::error;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use sqllogictest::TestError;

pub type Result<T> = std::result::Result<T, DozerSqlLogicTestError>;

#[derive(Debug, Error)]
pub enum DozerSqlLogicTestError {
    // Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(#[from] TestError),
    // Error from sqlite
    #[error("sqlite error: {0}")]
    SqliteError(#[from] rusqlite::Error),
    // Error from sqlparser
    #[error("Sqlparser parser error: {0}")]
    Sqlparser(#[from] dozer_sql::sqlparser::parser::ParserError),
    // Error from pipeline
    #[error("Dozer pipeline execution error: {0}")]
    Pipeline(#[from] ExecutionError),
}
