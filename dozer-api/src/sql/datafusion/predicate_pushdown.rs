use std::ops::Deref;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Decimal128Type, Decimal256Type, DecimalType};
use datafusion::scalar::ScalarValue;
use datafusion_expr::{Between, BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use dozer_cache::cache::expression::{
    FilterExpression, Operator as CacheFilterOperator, QueryExpression,
};
use dozer_types::json_types::JsonValue;
use dozer_types::log::debug;

use crate::api_helper::get_records;
use crate::CacheEndpoint;

pub(crate) fn supports_predicates_pushdown(
    exprs: &[&Expr],
    cache_endpoint: Arc<CacheEndpoint>,
) -> Vec<TableProviderFilterPushDown> {
    // preliminary check for potential predicate pushdown support
    let results = exprs
        .iter()
        .copied()
        .map(supports_predicate_pushdown)
        .collect::<Vec<_>>();

    if results
        .iter()
        .all(|result| matches!(result, TableProviderFilterPushDown::Unsupported))
    {
        return results;
    }

    let supported_exprs = exprs
        .iter()
        .zip(results.iter())
        .filter_map(|(expr, result)| {
            (!matches!(result, TableProviderFilterPushDown::Unsupported)).then_some(expr)
        })
        .copied();

    // test a query with predicate pushdown to check if there are missing indexes
    let mut query_expr = QueryExpression::with_limit(0);
    query_expr.filter = predicate_pushdown(supported_exprs);
    let query_result = get_records(
        &cache_endpoint.cache_reader(),
        &mut query_expr,
        &cache_endpoint.endpoint.name,
        None,
    );

    if let Err(err) = query_result {
        debug!("Predicate pushdown failed due to {err}");
        vec![TableProviderFilterPushDown::Unsupported; exprs.len()]
    } else {
        results
    }
}

fn supports_predicate_pushdown(expr: &Expr) -> TableProviderFilterPushDown {
    let is_applicable = match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            matches!(
                op,
                Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
            ) && matches!((&**left, &**right),
                (Expr::Column(_), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(_)) if is_suitable_for_pushdown(v))
        }

        Expr::IsNull(expr) | Expr::IsTrue(expr) | Expr::IsFalse(expr) => {
            matches!(&**expr, Expr::Column(_))
        }

        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => {
            matches!(
                (&**expr, &**low, &**high),
                (Expr::Column(_), Expr::Literal(v1), Expr::Literal(v2)) if [v1, v2].into_iter().all(is_suitable_for_pushdown)
            )
        }

        _ => false,
    };

    debug!(
        "Predicate pushdown is {}possible for {expr:?}",
        if is_applicable { "" } else { "not " }
    );

    if is_applicable {
        TableProviderFilterPushDown::Exact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

pub(crate) fn predicate_pushdown<'a>(
    predicates: impl Iterator<Item = &'a Expr>,
) -> Option<FilterExpression> {
    let mut and_list = Vec::new();
    predicates.into_iter().for_each(|expr| match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (c, op, v) = match (&**left, &**right) {
                (Expr::Column(c), Expr::Literal(v)) => {
                    let op = match op {
                        Operator::Eq => CacheFilterOperator::EQ,
                        Operator::Lt => CacheFilterOperator::LT,
                        Operator::LtEq => CacheFilterOperator::LTE,
                        Operator::Gt => CacheFilterOperator::GT,
                        Operator::GtEq => CacheFilterOperator::GTE,
                        _ => unreachable!(),
                    };
                    (c, op, v)
                }
                (Expr::Literal(v), Expr::Column(c)) => {
                    let op = match op {
                        Operator::Eq => CacheFilterOperator::EQ,
                        Operator::Lt => CacheFilterOperator::GT,
                        Operator::LtEq => CacheFilterOperator::GTE,
                        Operator::Gt => CacheFilterOperator::LT,
                        Operator::GtEq => CacheFilterOperator::LTE,
                        _ => unreachable!(),
                    };
                    (c, op, v)
                }
                _ => unreachable!(),
            };

            let column = c.name.clone();
            let value = json_value_from_scalar_value(v.clone());
            and_list.push(FilterExpression::Simple(column, op, value))
        }

        Expr::IsNull(expr) => match &**expr {
            Expr::Column(c) => and_list.push(FilterExpression::Simple(
                c.name.clone(),
                CacheFilterOperator::EQ,
                JsonValue::NULL,
            )),
            _ => unreachable!(),
        },

        Expr::IsTrue(expr) => match &**expr {
            Expr::Column(c) => and_list.push(FilterExpression::Simple(
                c.name.clone(),
                CacheFilterOperator::EQ,
                true.into(),
            )),
            _ => unreachable!(),
        },

        Expr::IsFalse(expr) => match &**expr {
            Expr::Column(c) => and_list.push(FilterExpression::Simple(
                c.name.clone(),
                CacheFilterOperator::EQ,
                false.into(),
            )),
            _ => unreachable!(),
        },

        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => match (&**expr, &**low, &**high) {
            (Expr::Column(c), Expr::Literal(low), Expr::Literal(high)) => {
                let column = c.name.clone();
                let low = json_value_from_scalar_value(low.clone());
                let high = json_value_from_scalar_value(high.clone());
                and_list.push(FilterExpression::Simple(
                    column.clone(),
                    CacheFilterOperator::GTE,
                    low,
                ));
                and_list.push(FilterExpression::Simple(
                    column,
                    CacheFilterOperator::LTE,
                    high,
                ));
            }
            _ => unreachable!(),
        },

        _ => unreachable!(),
    });

    if and_list.is_empty() {
        None
    } else if and_list.len() == 1 {
        Some(and_list.pop().unwrap())
    } else {
        Some(FilterExpression::And(and_list))
    }
}

fn is_suitable_for_pushdown(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Null
        | ScalarValue::Boolean(_)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Utf8(_)
        | ScalarValue::LargeUtf8(_)
        | ScalarValue::Binary(_)
        | ScalarValue::FixedSizeBinary(_, _)
        | ScalarValue::LargeBinary(_) => true,

        ScalarValue::Float32(Some(f)) if f.is_finite() => true,
        ScalarValue::Float64(Some(f)) if f.is_finite() => true,

        _ => false,
    }
}

fn json_value_from_scalar_value(value: ScalarValue) -> JsonValue {
    let is_null = matches!(
        &value,
        ScalarValue::Null
            | ScalarValue::Boolean(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None)
            | ScalarValue::Decimal128(None, _, _)
            | ScalarValue::Decimal256(None, _, _)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Binary(None)
            | ScalarValue::FixedSizeBinary(_, None)
            | ScalarValue::LargeBinary(None)
    );
    if is_null {
        JsonValue::NULL
    } else {
        match value {
            ScalarValue::Null => JsonValue::NULL,
            ScalarValue::Boolean(Some(v)) => v.into(),
            ScalarValue::Float32(Some(v)) => v.into(),
            ScalarValue::Float64(Some(v)) => v.into(),
            ScalarValue::Int8(Some(v)) => v.into(),
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                <Decimal128Type as DecimalType>::format_decimal(v, precision, scale).into()
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                <Decimal256Type as DecimalType>::format_decimal(v, precision, scale).into()
            }
            ScalarValue::Int16(Some(v)) => v.into(),
            ScalarValue::Int32(Some(v)) => v.into(),
            ScalarValue::Int64(Some(v)) => v.into(),
            ScalarValue::UInt8(Some(v)) => v.into(),
            ScalarValue::UInt16(Some(v)) => v.into(),
            ScalarValue::UInt32(Some(v)) => v.into(),
            ScalarValue::UInt64(Some(v)) => v.into(),
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.into(),
            ScalarValue::Binary(Some(v))
            | ScalarValue::FixedSizeBinary(_, Some(v))
            | ScalarValue::LargeBinary(Some(v)) => String::from_utf8_lossy(&v).deref().into(),
            _ => unreachable!(),
        }
    }
}
