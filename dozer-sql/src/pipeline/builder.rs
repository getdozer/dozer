use std::rc::Rc;
use std::sync::Arc;

use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Query, Select, SelectItem, SetExpr, Statement, UnaryOperator,
    Value as SqlValue,
};
use sqlparser::ast::ObjectType::Schema;

use dozer_core::dag::dag::{Endpoint, NodeHandle, PortHandle};
use dozer_core::dag::dag::Dag;
use dozer_core::dag::dag::NodeType;
use dozer_core::dag::mt_executor::{DefaultPortHandle, MultiThreadedDagExecutor};
use dozer_core::dag::node::{ExecutionContext, NextStep, Processor, Sink, Source};
use dozer_core::dag::node::NextStep::Continue;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema as DozerSchema};

use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::expression::{Column, PhysicalExpression};
use crate::pipeline::processor::projection_builder::ProjectionBuilder;
use crate::pipeline::processor::selection::{SelectionBuilder, SelectionProcessor};

pub struct PipelineBuilder {
    schema: DozerSchema,
}

impl PipelineBuilder {
    pub fn new(schema: DozerSchema) -> PipelineBuilder {
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

        let projection_to_selection = dag.connect(
            Endpoint::new(2, DefaultPortHandle),
            Endpoint::new(3, DefaultPortHandle),
        );

        Ok((dag, 2, 3))
    }
}


