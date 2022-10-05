use sqlparser::ast::{Query, Select, SetExpr, Statement};

use dozer_core::dag::dag::{Endpoint, NodeHandle};
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_types::types::Schema;

use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::processor::projection_builder::ProjectionBuilder;
use crate::pipeline::processor::selection::SelectionBuilder;

pub struct PipelineBuilder {
    schema: Schema,
}

impl PipelineBuilder {
    pub fn new(schema: Schema) -> PipelineBuilder {
        Self {
            schema
        }
    }

    pub fn statement_to_pipeline(&self, statement: Statement) -> Result<(Dag, NodeHandle, NodeHandle)> {
        match statement {
            Statement::Query(query) => self.query_to_pipeline(*query),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    pub fn query_to_pipeline(&self, query: Query) -> Result<(Dag, NodeHandle, NodeHandle)> {
        self.set_expr_to_pipeline(*query.body)
    }

    fn set_expr_to_pipeline(&self, set_expr: SetExpr) -> Result<(Dag, NodeHandle, NodeHandle)> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_pipeline(*s),
            SetExpr::Query(q) => self.query_to_pipeline(*q),
            _ => Err(DozerSqlError::NotImplemented(
                "Unsupported Query.".to_string(),
            )),
        }
    }

    fn select_to_pipeline(&self, select: Select) -> Result<(Dag, NodeHandle, NodeHandle)> {


        // Select clause
        let projection = ProjectionBuilder::new(&self.schema).get_processor(select.projection)?;

        // Where clause
        let selection = SelectionBuilder::new(&self.schema).get_processor(select.selection)?;

        let mut dag = Dag::new();

        dag.add_node(NodeType::Processor(Box::new(projection)), 2);
        dag.add_node(NodeType::Processor(Box::new(selection)), 3);

        let _ = dag.connect(
            Endpoint::new(2, DefaultPortHandle),
            Endpoint::new(3, DefaultPortHandle),
        );

        Ok((dag, 2, 3))
    }
}
