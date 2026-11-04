//! Expressions for a filter whose membership inputs change independently.
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use dozer_core::node::PortHandle;
use dozer_sql_expression::{
    builder::ExpressionBuilder,
    execution::Expression,
    operator::{BinaryOperatorType, UnaryOperatorType},
    sqlparser::ast::{
        BinaryOperator, Expr, FunctionArg, FunctionArgExpr, Ident, Query, UnaryOperator, Value,
    },
};
use dozer_types::{
    models::udf_config::UdfConfig,
    types::{Field, FieldDefinition, FieldType, Record, Schema, SourceDefinition},
};
use tokio::runtime::Runtime;

use super::processor::Membership;
use crate::errors::PipelineError;

/// Visit the children supported by the existing scalar expression builder. Query
/// bodies are deliberately separate scopes, never children of the outer row.
pub(crate) fn children(expr: &mut Expr) -> Vec<&mut Expr> {
    match expr {
        Expr::BinaryOp { left, right, .. } => vec![left, right],
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::InSubquery { expr, .. } => vec![expr],
        Expr::Like { expr, pattern, .. } => vec![expr, pattern],
        Expr::InList { expr, list, .. } => {
            let mut out = vec![expr.as_mut()];
            out.extend(list.iter_mut());
            out
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            let mut out = vec![expr.as_mut()];
            out.extend(trim_what.iter_mut().map(Box::as_mut));
            out
        }
        Expr::Interval(interval) => vec![interval.value.as_mut()],
        Expr::Function(function) => function
            .args
            .iter_mut()
            .filter_map(|arg| match arg {
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(expr),
                    ..
                }
                | FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
                _ => None,
            })
            .collect(),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let mut out: Vec<_> = operand.iter_mut().map(Box::as_mut).collect();
            out.extend(conditions.iter_mut());
            out.extend(results.iter_mut());
            out.extend(else_result.iter_mut().map(Box::as_mut));
            out
        }
        _ => vec![],
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Plan {
    Scalar(Expr),
    Literal(Field),
    In {
        operand: Box<Plan>,
        port: PortHandle,
        negated: bool,
    },
    And(Box<Plan>, Box<Plan>),
    Or(Box<Plan>, Box<Plan>),
    Not(Box<Plan>),
    Is {
        operand: Box<Plan>,
        value: Field,
        negated: bool,
    },
    Case {
        operand: Option<Box<Plan>>,
        branches: Vec<(Plan, Plan)>,
        otherwise: Option<Box<Plan>>,
    },
    Apply {
        template: Expr,
        arguments: Vec<Plan>,
    },
}

impl Plan {
    pub(crate) fn parse(
        mut expression: Expr,
        add_query: &mut impl FnMut(Query) -> Result<PortHandle, PipelineError>,
    ) -> Result<Self, PipelineError> {
        let plan = match expression {
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Self::In {
                operand: Box::new(Self::parse(*expr, add_query)?),
                port: add_query(*subquery)?,
                negated,
            },
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => Self::And(
                Box::new(Self::parse(*left, add_query)?),
                Box::new(Self::parse(*right, add_query)?),
            ),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => Self::Or(
                Box::new(Self::parse(*left, add_query)?),
                Box::new(Self::parse(*right, add_query)?),
            ),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => Self::Not(Box::new(Self::parse(*expr, add_query)?)),
            Expr::Nested(expr) => Self::parse(*expr, add_query)?,
            Expr::Value(Value::Null) => Self::Literal(Field::Null),
            Expr::Value(Value::Boolean(value)) => Self::Literal(Field::Boolean(value)),
            Expr::IsNull(expr) | Expr::IsUnknown(expr) => Self::Is {
                operand: Box::new(Self::parse(*expr, add_query)?),
                value: Field::Null,
                negated: false,
            },
            Expr::IsNotNull(expr) | Expr::IsNotUnknown(expr) => Self::Is {
                operand: Box::new(Self::parse(*expr, add_query)?),
                value: Field::Null,
                negated: true,
            },
            Expr::IsTrue(expr) => Self::Is {
                operand: Box::new(Self::parse(*expr, add_query)?),
                value: Field::Boolean(true),
                negated: false,
            },
            Expr::IsFalse(expr) => Self::Is {
                operand: Box::new(Self::parse(*expr, add_query)?),
                value: Field::Boolean(false),
                negated: false,
            },
            Expr::IsNotTrue(expr) => Self::Is {
                operand: Box::new(Self::parse(*expr, add_query)?),
                value: Field::Boolean(true),
                negated: true,
            },
            Expr::IsNotFalse(expr) => Self::Is {
                operand: Box::new(Self::parse(*expr, add_query)?),
                value: Field::Boolean(false),
                negated: true,
            },
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Self::Case {
                operand: operand
                    .map(|expr| Self::parse(*expr, add_query).map(Box::new))
                    .transpose()?,
                branches: conditions
                    .into_iter()
                    .zip(results)
                    .map(|(condition, result)| {
                        Ok((
                            Self::parse(condition, add_query)?,
                            Self::parse(result, add_query)?,
                        ))
                    })
                    .collect::<Result<_, PipelineError>>()?,
                otherwise: else_result
                    .map(|expr| Self::parse(*expr, add_query).map(Box::new))
                    .transpose()?,
            },
            _ => {
                let original = expression.clone();
                let mut arguments = vec![];
                for child in children(&mut expression) {
                    let placeholder =
                        Expr::Identifier(Ident::new(format!("argument_{}", arguments.len())));
                    let argument = std::mem::replace(child, placeholder);
                    arguments.push(Self::parse(argument, add_query)?);
                }
                if !arguments.iter().any(Self::needs_nullable_evaluation) {
                    Self::Scalar(original)
                } else {
                    Self::Apply {
                        template: expression,
                        arguments,
                    }
                }
            }
        };
        Ok(plan)
    }

    fn needs_nullable_evaluation(&self) -> bool {
        !matches!(self, Self::Scalar(_) | Self::Literal(_))
    }

    pub(crate) fn compile<'a>(
        &'a self,
        outer: &'a Schema,
        inputs: &'a HashMap<PortHandle, Schema>,
        udfs: &'a [UdfConfig],
        runtime: Arc<Runtime>,
    ) -> Pin<Box<dyn Future<Output = Result<TypedPredicate, PipelineError>> + Send + 'a>> {
        Box::pin(async move {
            let boolean = Some(FieldType::Boolean);
            let (predicate, typ) = match self {
                Self::Scalar(sql) => {
                    validate_repeatable(&mut sql.clone(), udfs)?;
                    validate_columns(sql, outer)?;
                    let expression = ExpressionBuilder::new(outer.fields.len(), runtime)
                        .build(false, sql, outer, udfs)
                        .await?;
                    let typ = match expression.get_type(outer) {
                        Ok(typ) => Some(typ.return_type),
                        Err(dozer_sql_expression::error::Error::LiteralExpressionIsNull) => None,
                        Err(error) => return Err(error.into()),
                    };
                    (Predicate::Scalar(expression), typ)
                }
                Self::Literal(value) => (
                    Predicate::Scalar(Expression::Literal(value.clone())),
                    value.ty(),
                ),
                Self::In {
                    operand,
                    port,
                    negated,
                } => {
                    let operand = operand.compile(outer, inputs, udfs, runtime).await?;
                    let inner = inputs
                        .get(port)
                        .ok_or(PipelineError::InvalidPortHandle(*port))?;
                    if inner.fields.len() != 1 {
                        return Err(PipelineError::InvalidQuery(
                            "IN subquery must return exactly one column".into(),
                        ));
                    }
                    if let Some(typ) = &operand.typ {
                        if !compatible_types(typ, &inner.fields[0].typ) {
                            return Err(PipelineError::InvalidQuery(format!(
                                "IN subquery types are incompatible: {typ} and {}",
                                inner.fields[0].typ
                            )));
                        }
                    }
                    (
                        Predicate::In {
                            operand: Box::new(operand.predicate),
                            index: usize::from(*port) - 1,
                            negated: *negated,
                        },
                        boolean,
                    )
                }
                Self::And(left, right) | Self::Or(left, right) => {
                    let left = left.compile(outer, inputs, udfs, runtime.clone()).await?;
                    let right = right.compile(outer, inputs, udfs, runtime).await?;
                    require_boolean(&left)?;
                    require_boolean(&right)?;
                    (
                        Predicate::Logical {
                            left: Box::new(left.predicate),
                            right: Box::new(right.predicate),
                            and: matches!(self, Self::And(..)),
                        },
                        boolean,
                    )
                }
                Self::Not(operand) => {
                    let operand = operand.compile(outer, inputs, udfs, runtime).await?;
                    require_boolean(&operand)?;
                    (Predicate::Not(Box::new(operand.predicate)), boolean)
                }
                Self::Is {
                    operand,
                    value,
                    negated,
                } => {
                    let operand = operand.compile(outer, inputs, udfs, runtime).await?;
                    if *value != Field::Null {
                        require_boolean(&operand)?;
                    }
                    (
                        Predicate::Is {
                            operand: Box::new(operand.predicate),
                            value: value.clone(),
                            negated: *negated,
                        },
                        boolean,
                    )
                }
                Self::Case {
                    operand,
                    branches,
                    otherwise,
                } => {
                    let operand = match operand {
                        Some(expr) => Some(Box::new(
                            expr.compile(outer, inputs, udfs, runtime.clone())
                                .await?
                                .predicate,
                        )),
                        None => None,
                    };
                    let mut compiled = vec![];
                    let mut typ = None;
                    for (condition, result) in branches {
                        let condition = condition
                            .compile(outer, inputs, udfs, runtime.clone())
                            .await?;
                        if operand.is_none() {
                            require_boolean(&condition)?;
                        }
                        let result = result.compile(outer, inputs, udfs, runtime.clone()).await?;
                        check_case_type(&typ, &result.typ)?;
                        if typ.is_none() {
                            typ = result.typ;
                        }
                        compiled.push((condition.predicate, result.predicate));
                    }
                    let otherwise = match otherwise {
                        Some(expr) => {
                            let expr = expr.compile(outer, inputs, udfs, runtime).await?;
                            check_case_type(&typ, &expr.typ)?;
                            if typ.is_none() {
                                typ = expr.typ;
                            }
                            Some(Box::new(expr.predicate))
                        }
                        None => None,
                    };
                    (
                        Predicate::Case {
                            operand,
                            branches: compiled,
                            otherwise,
                        },
                        typ,
                    )
                }
                Self::Apply {
                    template,
                    arguments,
                } => {
                    validate_repeatable(&mut template.clone(), udfs)?;
                    let mut compiled = vec![];
                    let mut schema = Schema::default();
                    for (index, argument) in arguments.iter().enumerate() {
                        let argument = argument
                            .compile(outer, inputs, udfs, runtime.clone())
                            .await?;
                        schema.fields.push(FieldDefinition::new(
                            format!("argument_{index}"),
                            argument.typ.unwrap_or(FieldType::Boolean),
                            true,
                            SourceDefinition::Dynamic,
                        ));
                        compiled.push(argument.predicate);
                    }
                    let expression = ExpressionBuilder::new(schema.fields.len(), runtime)
                        .build(false, template, &schema, udfs)
                        .await?;
                    let typ = expression.get_type(&schema)?.return_type;
                    (
                        Predicate::Apply {
                            expression,
                            schema,
                            arguments: compiled,
                        },
                        Some(typ),
                    )
                }
            };
            Ok(TypedPredicate { predicate, typ })
        })
    }
}

pub(crate) struct TypedPredicate {
    pub predicate: Predicate,
    pub typ: Option<FieldType>,
}

/// The legacy resolver accepts a unique name before looking at its qualifier.
/// Validate qualified references against the actual local schema first, without
/// changing the behavior of existing static-only queries.
pub(crate) fn validate_columns(expr: &Expr, schema: &Schema) -> Result<(), PipelineError> {
    if let Expr::CompoundIdentifier(parts) = expr {
        let matches = schema
            .fields
            .iter()
            .filter(|field| {
                if parts.last().map(|part| part.value.as_str()) != Some(field.name.as_str()) {
                    return false;
                }
                match (&field.source, parts.as_slice()) {
                    (SourceDefinition::Alias { name }, [table, _]) => name == &table.value,
                    (SourceDefinition::Table { name, .. }, [table, _]) => name == &table.value,
                    (SourceDefinition::Table { connection, name }, [source, table, _]) => {
                        connection == &source.value && name == &table.value
                    }
                    _ => false,
                }
            })
            .count();
        if matches != 1 {
            return Err(PipelineError::InvalidQuery(format!(
                "Qualified column {expr} is not uniquely defined in the local subquery schema"
            )));
        }
    }
    for child in children(&mut expr.clone()) {
        validate_columns(child, schema)?;
    }
    Ok(())
}

fn check_case_type(
    expected: &Option<FieldType>,
    actual: &Option<FieldType>,
) -> Result<(), PipelineError> {
    if let (Some(expected), Some(actual)) = (expected, actual) {
        if !compatible_types(expected, actual) {
            return Err(PipelineError::InvalidQuery(
                "Incompatible CASE result types in dynamic predicate".into(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_repeatable(
    expr: &mut Expr,
    udfs: &[UdfConfig],
) -> Result<(), PipelineError> {
    if let Expr::Function(function) = expr {
        let name = function.name.to_string();
        if name.eq_ignore_ascii_case("now")
            || udfs.iter().any(|udf| udf.name.eq_ignore_ascii_case(&name))
        {
            return Err(PipelineError::InvalidQuery("Volatile functions and UDFs are not supported in dynamic IN predicates or subqueries".into()));
        }
    }
    for child in children(expr) {
        validate_repeatable(child, udfs)?;
    }
    Ok(())
}

pub(crate) fn require_boolean(predicate: &TypedPredicate) -> Result<(), PipelineError> {
    if predicate.typ.is_some() && predicate.typ != Some(FieldType::Boolean) {
        return Err(PipelineError::InvalidQuery(
            "Dynamic WHERE requires boolean predicates".into(),
        ));
    }
    Ok(())
}

fn compatible_types(left: &FieldType, right: &FieldType) -> bool {
    use FieldType::*;
    let numeric =
        |typ: &FieldType| matches!(typ, UInt | U128 | Int | Int8 | I128 | Float | Decimal);
    // Avoid data-dependent string/numeric casts and unsupported binary/JSON comparisons.
    (left == right && !matches!(left, Binary | Json))
        || (numeric(left) && numeric(right))
        || (matches!(left, String | Text) && matches!(right, String | Text))
}

#[derive(Debug)]
pub(crate) enum Predicate {
    Scalar(Expression),
    In {
        operand: Box<Predicate>,
        index: usize,
        negated: bool,
    },
    Logical {
        left: Box<Predicate>,
        right: Box<Predicate>,
        and: bool,
    },
    Not(Box<Predicate>),
    Is {
        operand: Box<Predicate>,
        value: Field,
        negated: bool,
    },
    Case {
        operand: Option<Box<Predicate>>,
        branches: Vec<(Predicate, Predicate)>,
        otherwise: Option<Box<Predicate>>,
    },
    Apply {
        expression: Expression,
        schema: Schema,
        arguments: Vec<Predicate>,
    },
}

pub(crate) fn equal(left: Field, right: Field) -> Result<Field, PipelineError> {
    Ok(BinaryOperatorType::Eq.evaluate(
        &Schema::default(),
        &mut Expression::Literal(left),
        &mut Expression::Literal(right),
        &Record::default(),
    )?)
}

impl Predicate {
    pub(crate) fn evaluate(
        &mut self,
        record: &Record,
        schema: &Schema,
        inputs: &[Membership],
    ) -> Result<Field, PipelineError> {
        match self {
            Self::Scalar(expression) => Ok(expression.evaluate(record, schema)?),
            Self::In {
                operand,
                index,
                negated,
            } => {
                let value = operand.evaluate(record, schema, inputs)?;
                let result = inputs[*index].contains(value)?;
                Ok(match (result, *negated) {
                    (Field::Boolean(value), true) => Field::Boolean(!value),
                    (value, _) => value,
                })
            }
            Self::Logical { left, right, and } => {
                let left = left.evaluate(record, schema, inputs)?;
                let right = right.evaluate(record, schema, inputs)?;
                Ok(match (left, right, *and) {
                    (Field::Boolean(false), _, true) | (_, Field::Boolean(false), true) => {
                        Field::Boolean(false)
                    }
                    (Field::Boolean(true), _, false) | (_, Field::Boolean(true), false) => {
                        Field::Boolean(true)
                    }
                    (Field::Null, _, _) | (_, Field::Null, _) => Field::Null,
                    (Field::Boolean(left), Field::Boolean(right), true) => {
                        Field::Boolean(left && right)
                    }
                    (Field::Boolean(left), Field::Boolean(right), false) => {
                        Field::Boolean(left || right)
                    }
                    _ => {
                        return Err(PipelineError::InvalidQuery(
                            "Non-boolean dynamic predicate".into(),
                        ))
                    }
                })
            }
            Self::Not(operand) => {
                let value = operand.evaluate(record, schema, inputs)?;
                Ok(UnaryOperatorType::Not.evaluate(
                    &Schema::default(),
                    &mut Expression::Literal(value),
                    &Record::default(),
                )?)
            }
            Self::Is {
                operand,
                value,
                negated,
            } => Ok(Field::Boolean(
                (operand.evaluate(record, schema, inputs)? == *value) != *negated,
            )),
            Self::Case {
                operand,
                branches,
                otherwise,
            } => {
                let value = match operand {
                    Some(expr) => Some(expr.evaluate(record, schema, inputs)?),
                    None => None,
                };
                for (condition, result) in branches {
                    let condition = condition.evaluate(record, schema, inputs)?;
                    let matched = match &value {
                        Some(value) => equal(value.clone(), condition)?,
                        None => condition,
                    };
                    if matched == Field::Boolean(true) {
                        return result.evaluate(record, schema, inputs);
                    }
                }
                match otherwise {
                    Some(expr) => expr.evaluate(record, schema, inputs),
                    None => Ok(Field::Null),
                }
            }
            Self::Apply {
                expression,
                schema: argument_schema,
                arguments,
            } => {
                let values = arguments
                    .iter_mut()
                    .map(|expr| expr.evaluate(record, schema, inputs))
                    .collect::<Result<_, _>>()?;
                Ok(expression.evaluate(&Record::new(values), argument_schema)?)
            }
        }
    }
}
