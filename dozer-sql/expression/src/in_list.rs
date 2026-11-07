use dozer_types::types::{Field, Record, Schema};

use crate::comparison::evaluate_eq;
use crate::error::Error;
use crate::execution::Expression;

// Macro wrapper preserves the 5-argument crate-internal call site in execution.rs
// while delegating evaluation to a cohesive context-based implementation.
macro_rules! define_in_list_entrypoint {
    () => {
        pub(crate) fn evaluate_in_list(
            schema: &Schema,
            expr: &mut Expression,
            list: &mut [Expression],
            negated: bool,
            record: &Record,
        ) -> Result<Field, Error> {
            let mut query = InListQuery {
                expr,
                list,
                negated,
            };
            let ctx = EvalRef { schema, record };
            evaluate_in_list_query(&mut query, &ctx)
        }
    };
}

struct InListQuery<'a> {
    expr: &'a mut Expression,
    list: &'a mut [Expression],
    negated: bool,
}

struct EvalRef<'a> {
    schema: &'a Schema,
    record: &'a Record,
}

// Evaluate SQL IN / NOT IN list membership with cross-type comparison coercion
// and three-valued SQL NULL propagation when no candidate matches.
fn evaluate_in_list_query(query: &mut InListQuery<'_>, ctx: &EvalRef<'_>) -> Result<Field, Error> {
    let target = query.expr.evaluate(ctx.record, ctx.schema)?;
    if target == Field::Null {
        return Ok(resolve_null_target(query.list.is_empty(), query.negated));
    }

    let (matched, saw_null) = scan_candidates(&target, query.list, ctx)?;
    if matched {
        return Ok(Field::Boolean(!query.negated));
    }
    if saw_null {
        return Ok(Field::Null);
    }
    Ok(Field::Boolean(query.negated))
}

fn resolve_null_target(is_empty: bool, negated: bool) -> Field {
    if is_empty {
        Field::Boolean(negated)
    } else {
        Field::Null
    }
}

fn scan_candidates(
    target: &Field,
    list: &mut [Expression],
    ctx: &EvalRef<'_>,
) -> Result<(bool, bool), Error> {
    let mut saw_null = false;
    for candidate in list.iter_mut() {
        match matches_candidate(target, candidate, ctx)? {
            Some(true) => return Ok((true, saw_null)),
            Some(false) => {}
            None => saw_null = true,
        }
    }
    Ok((false, saw_null))
}

// Compare a non-null target field against a candidate list expression using
// Dozer equality coercion, falling back to direct equality for non-ordered types.
fn matches_candidate(
    target: &Field,
    candidate: &mut Expression,
    ctx: &EvalRef<'_>,
) -> Result<Option<bool>, Error> {
    let item = candidate.evaluate(ctx.record, ctx.schema)?;
    if item == Field::Null {
        return Ok(None);
    }
    if target == &item {
        return Ok(Some(true));
    }
    let mut lhs = Expression::Literal(target.clone());
    let mut rhs = Expression::Literal(item);
    match evaluate_eq(ctx.schema, &mut lhs, &mut rhs, ctx.record)? {
        Field::Boolean(matched) => Ok(Some(matched)),
        Field::Null => Ok(None),
        _ => Ok(Some(false)),
    }
}

define_in_list_entrypoint!();
