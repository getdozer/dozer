use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::operator::BinaryOperatorType;
use crate::pipeline::expression::scalar::common::ScalarFunctionType;
use crate::pipeline::tests::utils::get_select;

use crate::pipeline::expression::aggregate::AggregateFunctionType;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};
use sqlparser::ast::SelectItem;

#[test]
fn test_simple_function() {
    let sql = "SELECT CONCAT(a, b) FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![]
        }
    );
    assert_eq!(
        e,
        Expression::ScalarFunction {
            fun: ScalarFunctionType::Concat,
            args: vec![
                Expression::Column { index: 0 },
                Expression::Column { index: 1 }
            ]
        }
    );
}

#[test]
fn test_simple_aggr_function() {
    let sql = "SELECT SUM(field0) FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![Expression::AggregateFunction {
                fun: AggregateFunctionType::Sum,
                args: vec![Expression::Column { index: 0 }]
            }]
        }
    );
    assert_eq!(e, Expression::Column { index: 1 });
}

#[test]
fn test_2_nested_aggr_function() {
    let sql = "SELECT SUM(ROUND(field1, 2)) FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![Expression::AggregateFunction {
                fun: AggregateFunctionType::Sum,
                args: vec![Expression::ScalarFunction {
                    fun: ScalarFunctionType::Round,
                    args: vec![
                        Expression::Column { index: 1 },
                        Expression::Literal(Field::Int(2))
                    ]
                }]
            }]
        }
    );
    assert_eq!(e, Expression::Column { index: 2 });
}

#[test]
fn test_3_nested_aggr_function() {
    let sql = "SELECT ROUND(SUM(ROUND(field1, 2))) FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![Expression::AggregateFunction {
                fun: AggregateFunctionType::Sum,
                args: vec![Expression::ScalarFunction {
                    fun: ScalarFunctionType::Round,
                    args: vec![
                        Expression::Column { index: 1 },
                        Expression::Literal(Field::Int(2))
                    ]
                }]
            }]
        }
    );
    assert_eq!(
        e,
        Expression::ScalarFunction {
            fun: ScalarFunctionType::Round,
            args: vec![Expression::Column { index: 2 }]
        }
    );
}

#[test]
fn test_3_nested_aggr_function_dup() {
    let sql = "SELECT CONCAT(SUM(ROUND(field1, 2)), SUM(ROUND(field1, 2))) FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![Expression::AggregateFunction {
                fun: AggregateFunctionType::Sum,
                args: vec![Expression::ScalarFunction {
                    fun: ScalarFunctionType::Round,
                    args: vec![
                        Expression::Column { index: 1 },
                        Expression::Literal(Field::Int(2))
                    ]
                }]
            }]
        }
    );
    assert_eq!(
        e,
        Expression::ScalarFunction {
            fun: ScalarFunctionType::Concat,
            args: vec![
                Expression::Column { index: 2 },
                Expression::Column { index: 2 }
            ]
        }
    );
}

#[test]
fn test_3_nested_aggr_function_and_sum() {
    let sql = "SELECT ROUND(SUM(ROUND(field1, 2))) + SUM(field0) FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![
                Expression::AggregateFunction {
                    fun: AggregateFunctionType::Sum,
                    args: vec![Expression::ScalarFunction {
                        fun: ScalarFunctionType::Round,
                        args: vec![
                            Expression::Column { index: 1 },
                            Expression::Literal(Field::Int(2))
                        ]
                    }]
                },
                Expression::AggregateFunction {
                    fun: AggregateFunctionType::Sum,
                    args: vec![Expression::Column { index: 0 }]
                }
            ]
        }
    );
    assert_eq!(
        e,
        Expression::BinaryOperator {
            operator: BinaryOperatorType::Add,
            left: Box::new(Expression::ScalarFunction {
                fun: ScalarFunctionType::Round,
                args: vec![Expression::Column { index: 2 }]
            }),
            right: Box::new(Expression::Column { index: 3 })
        }
    );
}

#[test]
fn test_3_nested_aggr_function_and_sum_3() {
    let sql = "SELECT (ROUND(SUM(ROUND(field1, 2))) + SUM(field0)) + field0 FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![
                Expression::AggregateFunction {
                    fun: AggregateFunctionType::Sum,
                    args: vec![Expression::ScalarFunction {
                        fun: ScalarFunctionType::Round,
                        args: vec![
                            Expression::Column { index: 1 },
                            Expression::Literal(Field::Int(2))
                        ]
                    }]
                },
                Expression::AggregateFunction {
                    fun: AggregateFunctionType::Sum,
                    args: vec![Expression::Column { index: 0 }]
                }
            ]
        }
    );
    assert_eq!(
        e,
        Expression::BinaryOperator {
            operator: BinaryOperatorType::Add,
            left: Box::new(Expression::BinaryOperator {
                operator: BinaryOperatorType::Add,
                left: Box::new(Expression::ScalarFunction {
                    fun: ScalarFunctionType::Round,
                    args: vec![Expression::Column { index: 2 }]
                }),
                right: Box::new(Expression::Column { index: 3 })
            }),
            right: Box::new(Expression::Column { index: 0 })
        }
    );
}

#[test]
#[ignore]
#[should_panic]
fn test_wrong_nested_aggregations() {
    let sql = "SELECT SUM(SUM(field0)) FROM t0";
    let schema = Schema::default()
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

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let _e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };
}

#[test]
fn test_name_resolution() {
    let sql = "SELECT CONCAT(table0.a, connection1.table0.b, a) FROM t0";
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                "a".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Table {
                    connection: "connection1".to_string(),
                    name: "table0".to_string(),
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
                    connection: "connection1".to_string(),
                    name: "table0".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![]
        }
    );
    assert_eq!(
        e,
        Expression::ScalarFunction {
            fun: ScalarFunctionType::Concat,
            args: vec![
                Expression::Column { index: 0 },
                Expression::Column { index: 1 },
                Expression::Column { index: 0 }
            ]
        }
    );
}

#[test]
fn test_alias_resolution() {
    let sql = "SELECT CONCAT(alias.a, a) FROM t0";
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                "a".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Alias {
                    name: "alias".to_string(),
                },
            ),
            false,
        )
        .to_owned();

    let mut builder = ExpressionBuilder::new(schema.fields.len());
    let e = match &get_select(sql).unwrap().projection[0] {
        SelectItem::UnnamedExpr(e) => builder.build(true, e, &schema, &[]).unwrap(),
        _ => panic!("Invalid expr"),
    };

    assert_eq!(
        builder,
        ExpressionBuilder {
            offset: schema.fields.len(),
            aggregations: vec![]
        }
    );
    assert_eq!(
        e,
        Expression::ScalarFunction {
            fun: ScalarFunctionType::Concat,
            args: vec![
                Expression::Column { index: 0 },
                Expression::Column { index: 0 }
            ]
        }
    );
}
