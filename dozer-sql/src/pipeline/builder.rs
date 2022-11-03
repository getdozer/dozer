use super::processor::aggregation::AggregationProcessorFactory;
use super::processor::projection::ProjectionProcessorFactory;
use super::processor::selection::SelectionProcessorFactory;
use crate::common::utils::normalize_ident;
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::Endpoint;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_types::core::node::NodeHandle;
use dozer_types::errors::pipeline::PipelineError;
use dozer_types::errors::pipeline::PipelineError::{InvalidQuery, InvalidRelation};
use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor, TableWithJoins};
use std::collections::HashMap;

pub struct PipelineBuilder {}

impl PipelineBuilder {
    pub fn statement_to_pipeline(
        &self,
        statement: Statement,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint), PipelineError> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => Err(InvalidQuery),
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
            _ => Err(InvalidQuery),
        }
    }

    fn select_to_pipeline(
        &self,
        select: Select,
    ) -> Result<(Dag, HashMap<String, Endpoint>, Endpoint), PipelineError> {
        let mut dag = Dag::new();

        // The simplest query can contains just SELECT and FROM
        // Until the implementation of FROM clause, projection (or selection) comes first
        let mut first_node_name = String::from("projection");
        let mut last_node_name = String::from("projection");

        // Select clause
        let projection = ProjectionProcessorFactory::new(select.projection.clone());

        dag.add_node(
            NodeType::Processor(Box::new(projection)),
            String::from("projection"),
        );

        if let Some(selection) = select.selection {
            // Where clause
            let selection = SelectionProcessorFactory::new(Some(selection));
            first_node_name = String::from("selection");

            dag.add_node(
                NodeType::Processor(Box::new(selection)),
                String::from("selection"),
            );

            let _ = dag.connect(
                Endpoint::new(String::from("selection"), DEFAULT_PORT_HANDLE),
                Endpoint::new(String::from("projection"), DEFAULT_PORT_HANDLE),
            );
        }

        // Group by clause
        if !select.group_by.is_empty() {
            let aggregation =
                AggregationProcessorFactory::new(select.projection.clone(), select.group_by);

            last_node_name = String::from("aggregation");

            dag.add_node(
                NodeType::Processor(Box::new(aggregation)),
                String::from("aggregation"),
            );

            let _ = dag.connect(
                Endpoint::new(String::from("projection"), DEFAULT_PORT_HANDLE),
                Endpoint::new(String::from("aggregation"), DEFAULT_PORT_HANDLE),
            );
        }

        // From clause
        let input_endpoints = self.get_input_endpoints(&first_node_name, &select.from)?;

        Ok((
            dag,
            input_endpoints,
            Endpoint::new(last_node_name, DEFAULT_PORT_HANDLE),
        ))
    }

    fn get_input_endpoints(
        &self,
        node_name: &String,
        from: &[TableWithJoins],
    ) -> Result<HashMap<String, Endpoint>, PipelineError> {
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

    fn get_input_name(&self, table: &TableWithJoins) -> Result<String, PipelineError> {
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
            _ => Err(InvalidRelation),
        }
    }
}
