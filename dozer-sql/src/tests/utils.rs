use crate::errors::PipelineError;
use dozer_sql_expression::sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::DozerDialect,
    parser::Parser,
};

pub fn get_select(sql: &str) -> Result<Box<Select>, PipelineError> {
    let dialect = DozerDialect {};

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
        SetExpr::Select(select) => select,
        SetExpr::Query(query) => get_query_select(&query),
        _ => panic!("Only select queries are supported"),
    }
}
