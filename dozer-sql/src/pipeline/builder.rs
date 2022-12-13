use super::aggregation::factory::AggregationProcessorFactory;
use super::product::factory::get_input_tables;
use super::product::factory::ProductProcessorFactory;
use super::selection::factory::SelectionProcessorFactory;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidQuery;
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::Endpoint;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::{NodeHandle, PortHandle};
use sqlparser::ast::{Query, Select, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PipelineBuilder {
    namespace: Option<u16>,
}

impl PipelineBuilder {
    pub fn statement_to_pipeline(
        &self,
        statement: Statement,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint), PipelineError> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => Err(InvalidQuery(statement.to_string())),
        }
    }

    pub fn query_to_pipeline(
        &self,
        query: Query,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint), PipelineError> {
        self.set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(
        &self,
        set_expr: SetExpr,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint), PipelineError> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_pipeline(*s),
            SetExpr::Query(q) => self.query_to_pipeline(*q),
            _ => Err(InvalidQuery(set_expr.to_string())),
        }
    }

    fn select_to_pipeline(
        &self,
        select: Select,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint), PipelineError> {
        let mut dag = Dag::new();

        // FROM clause
        if select.from.len() != 1 {
            return Err(InvalidQuery(
                "FROM clause doesn't support \"Comma Syntax\"".to_string(),
            ));
        }
        let product = ProductProcessorFactory::new(select.from[0].clone());
        let input_tables = get_input_tables(&select.from[0])?;
        let input_endpoints =
            self.get_input_endpoints(self.namespace, String::from("product"), &input_tables)?;

        dag.add_node(
            NodeType::Processor(Arc::new(product)),
            NodeHandle::new(self.namespace, String::from("product")),
        );

        let aggregation =
            AggregationProcessorFactory::new(select.projection.clone(), select.group_by);

        dag.add_node(
            NodeType::Processor(Arc::new(aggregation)),
            NodeHandle::new(self.namespace, String::from("aggregation")),
        );

        // Where clause
        if let Some(selection) = select.selection {
            let selection = SelectionProcessorFactory::new(selection);
            // first_node_name = String::from("selection");

            dag.add_node(
                NodeType::Processor(Arc::new(selection)),
                NodeHandle::new(self.namespace, String::from("selection")),
            );

            let _ = dag.connect(
                Endpoint::new(
                    NodeHandle::new(self.namespace, String::from("product")),
                    DEFAULT_PORT_HANDLE,
                ),
                Endpoint::new(
                    NodeHandle::new(self.namespace, String::from("selection")),
                    DEFAULT_PORT_HANDLE,
                ),
            );

            let _ = dag.connect(
                Endpoint::new(
                    NodeHandle::new(self.namespace, String::from("selection")),
                    DEFAULT_PORT_HANDLE,
                ),
                Endpoint::new(
                    NodeHandle::new(self.namespace, String::from("aggregation")),
                    DEFAULT_PORT_HANDLE,
                ),
            );
        } else {
            let _ = dag.connect(
                Endpoint::new(
                    NodeHandle::new(self.namespace, String::from("product")),
                    DEFAULT_PORT_HANDLE,
                ),
                Endpoint::new(
                    NodeHandle::new(self.namespace, String::from("aggregation")),
                    DEFAULT_PORT_HANDLE,
                ),
            );
        }

        Ok((
            dag,
            input_endpoints,
            Endpoint::new(
                NodeHandle::new(self.namespace, String::from("aggregation")),
                DEFAULT_PORT_HANDLE,
            ),
        ))
    }

    fn get_input_endpoints(
        &self,
        namespace: Option<u16>,
        node_name: String,
        input_tables: &[String],
    ) -> Result<HashMap<String, Endpoint>, PipelineError> {
        let mut endpoints = HashMap::new();

        for (input_port, table) in input_tables.iter().enumerate() {
            endpoints.insert(
                table.clone(),
                Endpoint::new(
                    NodeHandle::new(namespace, node_name.clone()),
                    input_port as PortHandle,
                ),
            );
        }

        Ok(endpoints)
    }

    pub fn new(namespace: Option<u16>) -> Self {
        Self { namespace }
    }
}

pub fn get_select(sql: &str) -> Result<Box<Select>, PipelineError> {
    let statement = get_statement(sql);
    get_query(statement)
}

fn get_statement(sql: &str) -> Statement {
    let dialect = GenericDialect {};
    // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    ast[0].clone()
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
