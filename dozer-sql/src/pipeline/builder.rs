use anyhow::{bail, Result};
use std::collections::HashMap;

use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor, TableWithJoins};

use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::dag::{Endpoint, NodeHandle};
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_types::types::Schema;

use crate::common::utils::normalize_ident;
use crate::pipeline::processor::projection_builder::ProjectionBuilder;

use super::processor::selection::SelectionProcessorFactory;

pub struct PipelineBuilder {
    schema: Schema,
}

impl PipelineBuilder {
    pub fn new(schema: Schema) -> PipelineBuilder {
        Self { schema }
    }

    pub fn statement_to_pipeline(
        &self,
        statement: Statement,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint)> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => bail!("Unsupported Query."),
        }
    }

    pub fn query_to_pipeline(
        &self,
        query: Query,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint)> {
        self.set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(
        &self,
        set_expr: SetExpr,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint)> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_pipeline(*s),
            SetExpr::Query(q) => self.query_to_pipeline(*q),
            _ => bail!("Unsupported Query."),
        }
    }

    fn select_to_pipeline(
        &self,
        select: Select,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint)> {
        // From clause
        let input_endpoints = self.get_input_endpoints(&String::from("selection"), &select.from)?;

        // Where clause
        let selection = SelectionProcessorFactory::new(select.selection);

        // Select clause
        let projection = ProjectionBuilder::new(&self.schema).get_processor(&select.projection)?;

        let mut dag = Dag::new();

        dag.add_node(
            NodeType::Processor(Box::new(selection)),
            String::from("selection"),
        );
        dag.add_node(
            NodeType::Processor(Box::new(projection)),
            String::from("projection"),
        );

        let _ = dag.connect(
            Endpoint::new(String::from("selection"), DEFAULT_PORT_HANDLE),
            Endpoint::new(String::from("projection"), DEFAULT_PORT_HANDLE),
        );

        Ok((
            dag,
            input_endpoints,
            Endpoint::new(String::from("projection"), DEFAULT_PORT_HANDLE),
        ))
    }

    fn get_input_endpoints(
        &self,
        node_name: &String,
        from: &[TableWithJoins],
    ) -> Result<HashMap<String, Endpoint>> {
        let mut endpoints = HashMap::new();

        if from.len() != 1 {
            panic!("Change following implementation to support multiple inputs")
        }

        for (_input_port, table) in from.iter().enumerate() {
            let input_name = self.get_input_name(table).unwrap();
            endpoints.insert(
                input_name,
                Endpoint::new(NodeHandle::from(node_name), DEFAULT_PORT_HANDLE), // input_port as u16),
            );
        }

        Ok(endpoints)
    }

    fn get_input_name(&self, table: &TableWithJoins) -> Result<String> {
        match &table.relation {
            TableFactor::Table { name, alias: _, .. } => {
                let input_name = name
                    .0
                    .iter()
                    .map(normalize_ident)
                    .collect::<Vec<String>>()
                    .join(".");

                Ok(input_name)
            }
            _ => bail!("Unsupported Table Name."),
        }
    }
}
