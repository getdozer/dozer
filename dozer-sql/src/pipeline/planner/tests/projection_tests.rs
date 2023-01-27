use crate::pipeline::builder::QueryContext;
use crate::pipeline::expression::builder_new::ExpressionContext;
use crate::pipeline::planner::projection::{PrimaryKeyAction, ProjectionPlanner};
use crate::pipeline::projection::processor::ProjectionProcessor;
use crate::pipeline::tests::utils::get_select;
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};
use sqlparser::ast::{Query, Select, SelectItem, SetExpr, Statement};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

#[test]
fn test_basic_projection() {
    let sql = "SELECT CONCAT(a,b), a FROM t0";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "a".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Table {
                    name: "t0".to_string(),
                    connection: "c0".to_string(),
                },
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "b".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Table {
                    name: "t0".to_string(),
                    connection: "c0".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let mut projection_planner = ProjectionPlanner::new(schema);
    let select = get_select(sql).unwrap().projection;

    for expr in select {
        projection_planner
            .add_select_item(expr, PrimaryKeyAction::Retain)
            .unwrap();
    }
}
