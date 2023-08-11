use crate::pipeline::expression::aggregate::AggregateFunctionType;

use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::operator::BinaryOperatorType;
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::planner::projection::CommonPlanner;

use crate::pipeline::tests::utils::get_select;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_basic_projection() {
    let sql =
        "SELECT ROUND(SUM(ROUND(a,2)),2), a as a2 FROM t0 GROUP BY b,a HAVING SUM(ROUND(a,2)) > SUM(b)";
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                "a".to_string(),
                FieldType::Int,
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
                FieldType::Int,
                false,
                SourceDefinition::Table {
                    name: "t0".to_string(),
                    connection: "c0".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let mut projection_planner = CommonPlanner::new(schema, vec![]);
    let statement = get_select(sql).unwrap();

    projection_planner.plan(*statement).unwrap();

    assert_eq!(
        projection_planner.aggregation_output,
        vec![
            Expression::AggregateFunction {
                fun: AggregateFunctionType::Sum,
                args: vec![Expression::ScalarFunction {
                    fun: ScalarFunctionType::Round,
                    args: vec![
                        Expression::Column { index: 0 },
                        Expression::Literal(Field::Int(2))
                    ]
                }]
            },
            Expression::AggregateFunction {
                fun: AggregateFunctionType::Sum,
                args: vec![Expression::Column { index: 1 }]
            }
        ]
    );

    assert_eq!(
        projection_planner.projection_output,
        vec![
            Expression::ScalarFunction {
                fun: ScalarFunctionType::Round,
                args: vec![
                    Expression::Column { index: 2 },
                    Expression::Literal(Field::Int(2))
                ]
            },
            Expression::Column { index: 0 }
        ]
    );

    assert_eq!(
        projection_planner.post_projection_schema,
        Schema::default()
            .field(
                FieldDefinition::new(
                    "ROUND(SUM(ROUND(a,2)),2)".to_string(),
                    FieldType::Int,
                    true,
                    SourceDefinition::Dynamic
                ),
                false
            )
            .field(
                FieldDefinition::new(
                    "a2".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Table {
                        name: "t0".to_string(),
                        connection: "c0".to_string(),
                    },
                ),
                false,
            )
            .to_owned()
    );

    assert_eq!(
        projection_planner.groupby,
        vec![
            Expression::Column { index: 1 },
            Expression::Column { index: 0 }
        ]
    );

    assert_eq!(
        projection_planner.having,
        Some(Expression::BinaryOperator {
            operator: BinaryOperatorType::Gt,
            left: Box::new(Expression::Column { index: 2 }),
            right: Box::new(Expression::Column { index: 3 })
        })
    );
}
