use std::collections::HashMap;
use anyhow::bail;

use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor, TableWithJoins};

use dozer_core::dag::dag::{Endpoint, NodeHandle, PortHandle};
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_types::types::Schema;

use crate::common::error::{DozerSqlError, Result};
use crate::common::utils::normalize_ident;
use crate::pipeline::processor::projection_builder::ProjectionBuilder;
use crate::pipeline::processor::selection_builder::SelectionBuilder;

pub struct PipelineBuilder {
    schema: Schema,
}

impl PipelineBuilder {
    pub fn new(schema: Schema) -> PipelineBuilder {
        Self {
            schema
        }
    }

    pub fn statement_to_pipeline(&self, statement: Statement) -> Result<(Dag, HashMap<&str, Endpoint>, Endpoint)> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    pub fn query_to_pipeline(&self, query: Query) -> Result<(Dag, HashMap<&str, Endpoint>, Endpoint)> {
        self.set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(&self, set_expr: SetExpr) -> Result<(Dag, HashMap<&str, Endpoint>, Endpoint)> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_pipeline(*s),
            SetExpr::Query(q) => self.query_to_pipeline(*q),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    fn select_to_pipeline(&self, select: Select) -> Result<(Dag, HashMap<&str, Endpoint>, Endpoint)> {

        // From clause
        let input_endpoints = self.get_input_endpoints(&String::from("selection"), &select.from)?;

        // Where clause
        let selection = SelectionBuilder::new(&self.schema).get_processor(&select.selection, &select.from)?;

        // Select clause
        let projection = ProjectionBuilder::new(&self.schema).get_processor(&select.projection)?;


        let mut dag = Dag::new();

        dag.add_node(NodeType::Processor(Box::new(selection)), String::from("selection"));
        dag.add_node(NodeType::Processor(Box::new(projection)), String::from("projection"));

        let _ = dag.connect(
            Endpoint::new(String::from("selection"), DefaultPortHandle),
            Endpoint::new(String::from("projection"), DefaultPortHandle),
        );

        let input = HashMap::from([("default", Endpoint::new(String::from("selection"), DefaultPortHandle))]);

        Ok((dag, input, Endpoint::new(String::from("projection"), DefaultPortHandle)))
    }


    fn get_input_endpoints(&self, node_name: &String, from: &Vec<TableWithJoins>) -> Result<HashMap<String, Endpoint>> {
        let mut endpoints = HashMap::new();

        let mut input_port: u16 = 0;
        for table in from.into_iter() {
            let input_name = self.get_input_name(table).unwrap();
            endpoints.insert(input_name, Endpoint::new(NodeHandle::from(node_name), input_port));
            input_port += 1;
        }

        Ok(endpoints)

    }


    fn get_input_name(&self, table: &TableWithJoins) -> anyhow::Result<String> {
        match &table.relation {
            TableFactor::Table { name, alias, .. } => {
                let input_name = name.0.iter()
                    .map(normalize_ident)
                    .collect::<Vec<String>>()
                    .join(".");

                Ok(input_name)

            }
            _ => bail!("Unsupported Table Name.")
        }
    }

}
