use crate::pipeline::errors::PipelineError;
use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::AnsiDialect,
    parser::Parser,
};

pub fn get_select(sql: &str) -> Result<Box<Select>, PipelineError> {
    let dialect = AnsiDialect {};

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    let statement = ast.get(0).expect("First statement is missing").to_owned();
    if let Statement::Query(query) = statement {
        Ok(get_query_select(&query))
    } else {
        panic!("this is not supposed to be called");
    }
}

pub fn get_query_select(query: &Query) -> Box<Select> {
    match *query.body.clone() {
        SetExpr::Select(select) => {
            return select;
        }
        SetExpr::Query(query) => get_query_select(&query),
        _ => panic!("Only select queries are supported"),
    }
}
