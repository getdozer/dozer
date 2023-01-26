#![allow(dead_code)]

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::Expression;
use dozer_types::types::Schema;
use sqlparser::ast::{Expr, SelectItem};

struct ProjectionPlanner {
    input_schema: Schema,
    output_schema: Schema,
}

impl ProjectionPlanner {
    fn parse_projection_item(expr: Expr) -> Result<(), PipelineError> {
        Ok(())
    }
}
