use sqlparser::ast::{Expr as SqlExpr, Query, Select, SelectItem, SetExpr, Statement};
use crate::common::error::{DozerSqlError, Result};
use dozer_core::dag::dag::{Dag};
use dozer_core::dag::node::{Processor};

pub struct PipelineBuilder {}

impl PipelineBuilder {
    pub fn statement_to_pipeline(statement: Statement) -> Result<Dag> {
        match statement {
            Statement::Query(query) => PipelineBuilder::query_to_pipeline(*query),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    pub fn query_to_pipeline(query: Query) -> Result<Dag> {
        PipelineBuilder::set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(set_expr: SetExpr) -> Result<Dag> {
        match set_expr {
            SetExpr::Select(s) => PipelineBuilder::select_to_pipeline(*s),
            SetExpr::Query(q) => PipelineBuilder::query_to_pipeline(*q),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    fn select_to_pipeline(select: Select) -> Result<Dag> {
        // Where clause
        let selection_processor = PipelineBuilder::selection_to_processor(select.selection)?;

        // Select clause
        let projection_processor = PipelineBuilder::projection_to_processor(select.projection)?;


        Ok(Dag::new())
    }

    fn selection_to_processor(selection: Option<SqlExpr>) -> Result<Box<dyn Processor>> {
        Err(DozerSqlError::NotImplemented("Unsupported WHERE.".to_string()))
    }

    fn projection_to_processor(projection: Vec<SelectItem>) -> Result<Box<dyn Processor>> {
        Err(DozerSqlError::NotImplemented("Unsupported SELECT.".to_string()))
    }
}

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[test]
fn test_pipeline_builder() {

    let sql = "SELECT Country, COUNT(CustomerID), SUM(Spending) \
                            FROM Customers \
                            WHERE Spending >= 1000 \
                            GROUP BY Country \
                            HAVING COUNT(CustomerID) > 1;";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("AST: {:?}", ast);

    let statement = &ast[0];
    let pipeline = PipelineBuilder::statement_to_pipeline(statement.clone()).unwrap();
}