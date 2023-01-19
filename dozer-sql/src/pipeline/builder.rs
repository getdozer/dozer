use super::aggregation::factory::AggregationProcessorFactory;
use super::product::factory::get_input_tables;
use super::product::factory::ProductProcessorFactory;
use super::selection::factory::SelectionProcessorFactory;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidQuery;
use dozer_core::dag::app::AppPipeline;
use dozer_core::dag::app::PipelineEntryPoint;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::PortHandle;
use sqlparser::ast::{Query, Select, SetExpr, Statement};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

use dozer_core::dag::appsource::AppSourceId;

pub struct PipelineBuilder {}

impl PipelineBuilder {
    pub fn build_pipeline(&self, sql: &str) -> Result<AppPipeline, PipelineError> {
        let statement = get_statement(sql)?;
        let query = get_query(statement)?;
        self.select_to_pipeline(*query)
    }
    pub fn statement_to_pipeline(
        &self,
        statement: Statement,
    ) -> Result<AppPipeline, PipelineError> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => Err(InvalidQuery(statement.to_string())),
        }
    }

    pub fn query_to_pipeline(&self, query: Query) -> Result<AppPipeline, PipelineError> {
        self.set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(&self, set_expr: SetExpr) -> Result<AppPipeline, PipelineError> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_pipeline(*s),
            SetExpr::Query(q) => self.query_to_pipeline(*q),
            _ => Err(InvalidQuery(set_expr.to_string())),
        }
    }

    pub fn select_to_pipeline(&self, select: Select) -> Result<AppPipeline, PipelineError> {
        let mut pipeline = AppPipeline::new();

        // FROM clause
        if select.from.len() != 1 {
            return Err(InvalidQuery(
                "FROM clause doesn't support \"Comma Syntax\"".to_string(),
            ));
        }

        let input_tables = get_input_tables(&select.from[0])?;
        let product = ProductProcessorFactory::new(select.from[0].clone(), input_tables.clone());

        let input_endpoints = self.get_input_endpoints(&input_tables)?;

        pipeline.add_processor(Arc::new(product), "product", input_endpoints);

        let aggregation =
            AggregationProcessorFactory::new(select.projection.clone(), select.group_by);

        pipeline.add_processor(Arc::new(aggregation), "aggregation", vec![]);

        // Where clause
        if let Some(selection) = select.selection {
            let selection = SelectionProcessorFactory::new(selection);
            // first_node_name = String::from("selection");

            pipeline.add_processor(Arc::new(selection), "selection", vec![]);

            pipeline.connect_nodes(
                "product",
                Some(DEFAULT_PORT_HANDLE),
                "selection",
                Some(DEFAULT_PORT_HANDLE),
            )?;

            pipeline.connect_nodes(
                "selection",
                Some(DEFAULT_PORT_HANDLE),
                "aggregation",
                Some(DEFAULT_PORT_HANDLE),
            )?;
        } else {
            pipeline.connect_nodes(
                "product",
                Some(DEFAULT_PORT_HANDLE),
                "aggregation",
                Some(DEFAULT_PORT_HANDLE),
            )?;
        }

        Ok(pipeline)
    }

    pub fn get_input_endpoints(
        &self,
        input_tables: &[String],
    ) -> Result<Vec<PipelineEntryPoint>, PipelineError> {
        let mut endpoints = vec![];

        for (input_port, table) in input_tables.iter().enumerate() {
            endpoints.push(PipelineEntryPoint::new(
                AppSourceId::new(table.clone(), None),
                input_port as PortHandle,
            ));
        }

        Ok(endpoints)
    }
}

pub fn get_select(sql: &str) -> Result<Box<Select>, PipelineError> {
    let statement = get_statement(sql)?;
    get_query(statement)
}

fn get_statement(sql: &str) -> Result<Statement, PipelineError> {
    let mut ast = Parser::parse_sql(&AnsiDialect {}, sql)
        .map_err(|e| PipelineError::InvalidQuery(e.to_string()))?;
    if ast.is_empty() {
        return Err(InvalidQuery(
            "Query must have at least one statement".into(),
        ));
    }
    if ast.len() > 1 {
        return Err(InvalidQuery("Query must have only one statement".into()));
    }
    Ok(ast.remove(0))
}

pub fn statement_to_pipeline(statement: Statement) -> Result<Box<Select>, PipelineError> {
    match statement {
        Statement::Query(query) => get_body(*query),
        _ => Err(InvalidQuery(statement.to_string())),
    }
}

pub fn get_query(statement: Statement) -> Result<Box<Select>, PipelineError> {
    if let Statement::Query(query) = statement {
        get_body(*query)
    } else {
        Err(InvalidQuery(statement.to_string()))
    }
}

pub fn get_body(query: Query) -> Result<Box<Select>, PipelineError> {
    {
        let set_expr = *query.body;
        match set_expr {
            SetExpr::Select(s) => Ok(s),
            SetExpr::Query(q) => get_body(*q),
            _ => Err(InvalidQuery(set_expr.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::errors::PipelineError;

    use super::get_statement;

    fn assert_invalid_query(error: PipelineError, expected: &str) {
        match error {
            PipelineError::InvalidQuery(actual) => assert_eq!(actual, expected),
            _ => panic!("Expected InvalidQuery error"),
        }
    }

    #[test]
    fn test_get_statement() {
        assert_invalid_query(
            get_statement("").unwrap_err(),
            "Query must have at least one statement",
        );
        assert_invalid_query(
            get_statement(";").unwrap_err(),
            "Query must have at least one statement",
        );
        assert!(get_statement("SELECT * FROM foo").is_ok());
        assert!(get_statement("SELECT * FROM foo;").is_ok());
        assert!(get_statement("SELECT * FROM foo;;").is_ok());
        assert_invalid_query(
            get_statement("SELECT * FROM foo; SELECT * FROM foo").unwrap_err(),
            "Query must have only one statement",
        );
        assert_invalid_query(
            get_statement("SELECT * FROM foo; SELECT * FROM foo;").unwrap_err(),
            "Query must have only one statement",
        );
    }
}
