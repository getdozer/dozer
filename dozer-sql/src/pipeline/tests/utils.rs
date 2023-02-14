use sqlparser::{
    ast::{Query, Select, SetExpr, Statement},
    dialect::AnsiDialect,
    parser::Parser,
};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::product::set::SetOperation;

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
    return match *query.body.clone() {
        SetExpr::Select(select) => {
            return select;
        }
        SetExpr::Query(query) => get_query_select(&query),
        _ => panic!("Only select queries are supported"),
    };
}

pub fn get_set_operation(sql: &str) -> Result<SetOperation, PipelineError> {
    let dialect = AnsiDialect {};

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    let statement = ast.get(0).expect("First statement is missing").to_owned();
    if let Statement::Query(query) = statement {
        Ok(get_query_set_operation(&query))
    } else {
        panic!("this is not supposed to be called");
    }
}

pub fn get_query_set_operation(query: &Query) -> SetOperation {
    return match *query.body.clone() {
        SetExpr::SetOperation{ op, set_quantifier: _set_quantifier, left, right } => {
            match (*left, *right) {
                (SetExpr::Select(left_select), SetExpr::Select(right_select)) => {
                    SetOperation{ op, left: *left_select, right: *right_select }
                }
                _ => panic!("Only select queries are supported"),
            }
        }
        _ => panic!("This is not supposed to be called {:?}", *query.body),
    };
}