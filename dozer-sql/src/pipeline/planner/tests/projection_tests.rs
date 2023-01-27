use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::builder::QueryContext;
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::builder_new::ExpressionContext;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::planner::projection::{PrimaryKeyAction, ProjectionPlanner};
use crate::pipeline::projection::processor::ProjectionProcessor;
use crate::pipeline::tests::utils::get_select;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};
use sqlparser::ast::{Query, Select, SelectItem, SetExpr, Statement};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

#[test]
fn test_basic_projection() {
    let sql = "SELECT ROUND(SUM(ROUND(a,2)),2), a FROM t0";
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

    assert_eq!(
        projection_planner.aggregation_output,
        vec![Expression::AggregateFunction {
            fun: AggregateFunctionType::Sum,
            args: vec![Expression::ScalarFunction {
                fun: ScalarFunctionType::Round,
                args: vec![
                    Expression::Column { index: 0 },
                    Expression::Literal(Field::Int(2))
                ]
            }]
        }]
    );
}
