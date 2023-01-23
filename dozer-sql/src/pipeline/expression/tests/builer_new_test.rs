use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::expression::builder_new::{
    AggregationMeasure, ExpressionBuilder, ExpressionContext,
};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::operator::BinaryOperatorType;
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::tests::utils::get_select;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition};
use sqlparser::ast::SelectItem;

#[test]
fn test_simple_function() {
    let sql = "SELECT CONCAT(a,b) FROM t0";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "a".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "b".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .to_owned();

    let mut context = ExpressionContext::new();
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, &e, &schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        context,
        ExpressionContext {
            aggrgeations: vec![]
        }
    );
    assert_eq!(
        e,
        Box::new(Expression::ScalarFunction {
            fun: ScalarFunctionType::Concat,
            args: vec![
                Expression::Column { index: 0 },
                Expression::Column { index: 1 }
            ]
        })
    );
}

#[test]
fn test_simple_aggr_function() {
    let sql = "SELECT SUM(field0) FROM t0";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "field0".to_string(),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .to_owned();

    let mut context = ExpressionContext::new();
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, &e, &schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        context,
        ExpressionContext {
            aggrgeations: vec![AggregationMeasure {
                typ: Aggregator::Sum,
                arg: Expression::Column { index: 0 }
            }]
        }
    );
    assert_eq!(e, Box::new(Expression::Column { index: 0 }));
}

#[test]
fn test_2_nested_aggr_function() {
    let sql = "SELECT SUM(ROUND(field1,2)) FROM t0";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "field0".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "field1".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .to_owned();

    let mut context = ExpressionContext::new();
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, &e, &schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        context,
        ExpressionContext {
            aggrgeations: vec![AggregationMeasure {
                typ: Aggregator::Sum,
                arg: Expression::ScalarFunction {
                    fun: ScalarFunctionType::Round,
                    args: vec![
                        Expression::Column { index: 1 },
                        Expression::Literal(Field::Int(2))
                    ]
                }
            }]
        }
    );
    assert_eq!(e, Box::new(Expression::Column { index: 0 }));
}

#[test]
fn test_3_nested_aggr_function() {
    let sql = "SELECT ROUND(SUM(ROUND(field1,2))) FROM t0";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "field0".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "field1".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .to_owned();

    let mut context = ExpressionContext::new();
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, &e, &schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        context,
        ExpressionContext {
            aggrgeations: vec![AggregationMeasure {
                typ: Aggregator::Sum,
                arg: Expression::ScalarFunction {
                    fun: ScalarFunctionType::Round,
                    args: vec![
                        Expression::Column { index: 1 },
                        Expression::Literal(Field::Int(2))
                    ]
                }
            }]
        }
    );
    assert_eq!(
        e,
        Box::new(Expression::ScalarFunction {
            fun: ScalarFunctionType::Round,
            args: vec![Expression::Column { index: 0 }]
        })
    );
}

#[test]
fn test_3_nested_aggr_function_and_sum() {
    let sql = "SELECT ROUND(SUM(ROUND(field1,2))) + SUM(field0) FROM t0";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "field0".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "field1".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .to_owned();

    let mut context = ExpressionContext::new();
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, &e, &schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        context,
        ExpressionContext {
            aggrgeations: vec![
                AggregationMeasure {
                    typ: Aggregator::Sum,
                    arg: Expression::ScalarFunction {
                        fun: ScalarFunctionType::Round,
                        args: vec![
                            Expression::Column { index: 1 },
                            Expression::Literal(Field::Int(2))
                        ]
                    }
                },
                AggregationMeasure {
                    typ: Aggregator::Sum,
                    arg: Expression::Column { index: 0 }
                }
            ]
        }
    );
    assert_eq!(
        e,
        Box::new(Expression::BinaryOperator {
            operator: BinaryOperatorType::Add,
            left: Box::new(Expression::ScalarFunction {
                fun: ScalarFunctionType::Round,
                args: vec![Expression::Column { index: 0 }]
            }),
            right: Box::new(Expression::Column { index: 1 })
        })
    );
}

#[test]
#[should_panic]
fn test_wrong_nested_aggregations() {
    let sql = "SELECT SUM(SUM(field0)) FROM t0";
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                "field0".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .to_owned();

    let mut context = ExpressionContext::new();
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => {
            ExpressionBuilder::build(&mut context, true, &e, &schema).unwrap()
        }
        _ => panic!("Invalid expr"),
    };
}
