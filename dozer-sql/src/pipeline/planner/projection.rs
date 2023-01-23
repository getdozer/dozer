use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder_new::ExpressionBuilder;
use crate::pipeline::expression::execution::Expression;
use dozer_types::types::Schema;
use sqlparser::ast::SelectItem;

enum FieldReferenceType {
    Alias(String),
    Name(String),
}

struct ProjectionPlanner {
    input_schema: Schema,
}

impl ProjectionPlanner {
    // fn parse_projection_item(item: &SelectItem) -> Result<Vec<(FieldReferenceType, Expression)>, PipelineError> {
    //     match item {
    //         SelectItem::UnnamedExpr(_) => ExpressionBuilder::build(),
    //         SelectItem::ExprWithAlias { .. } => {}
    //         SelectItem::QualifiedWildcard(_, _) => {}
    //         SelectItem::Wildcard(_) => {}
    //     }
    // }
}
