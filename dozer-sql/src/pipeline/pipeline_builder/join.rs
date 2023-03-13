use dozer_core::app::AppPipeline;
use sqlparser::ast::TableWithJoins;

use crate::pipeline::{
    builder::{QueryContext, SchemaSQLContext},
    errors::PipelineError,
};

use super::from::ConnectionInfo;

pub(crate) fn insert_join_to_pipeline(
    from: &TableWithJoins,
    pipeline: &mut AppPipeline<SchemaSQLContext>,
    pipeline_idx: usize,
    query_context: &mut QueryContext,
) -> Result<ConnectionInfo, PipelineError> {
    todo!()
}
