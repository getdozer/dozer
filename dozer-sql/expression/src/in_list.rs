use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

use crate::comparison::evaluate_eq;
use crate::error::Error;
use crate::execution::Expression;

pub(crate) fn evaluate_in_list(
    schema: &Schema,
    expr: &mut Expression,
    list: &mut [Expression],
    negated: bool,
    record: &Record,
) -> Result<Field, Error> {
    let field = expr.evaluate(record, schema)?;
    if matches!(field, Field::Null) {
        return Ok(Field::Null);
    }

    let mut has_null = false;
    for item in list {
        let mut left = Expression::Literal(field.clone());
        match evaluate_eq(schema, &mut left, item, record)? {
            Field::Boolean(true) => return Ok(Field::Boolean(!negated)),
            Field::Boolean(false) => {}
            Field::Null => has_null = true,
            _ => {}
        }
    }

    if has_null {
        Ok(Field::Null)
    } else {
        Ok(Field::Boolean(negated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::ordered_float::OrderedFloat;

    #[test]
    fn test_in_list_cross_type_numeric_coercion() {
        let schema = Schema::new();
        let record = Record::new(vec![]);

        let mut expr = Expression::Literal(Field::Int(1));
        let mut list = vec![
            Expression::Literal(Field::UInt(2)),
            Expression::Literal(Field::Float(OrderedFloat(1.0))),
        ];

        assert_eq!(
            evaluate_in_list(&schema, &mut expr, &mut list, false, &record).unwrap(),
            Field::Boolean(true)
        );
        assert_eq!(
            evaluate_in_list(&schema, &mut expr, &mut list, true, &record).unwrap(),
            Field::Boolean(false)
        );
    }

    #[test]
    fn test_in_list_three_valued_null_semantics() {
        let schema = Schema::new();
        let record = Record::new(vec![]);

        let mut null_expr = Expression::Literal(Field::Null);
        let mut list = vec![
            Expression::Literal(Field::Int(1)),
            Expression::Literal(Field::Int(2)),
        ];
        assert_eq!(
            evaluate_in_list(&schema, &mut null_expr, &mut list, false, &record).unwrap(),
            Field::Null
        );
        assert_eq!(
            evaluate_in_list(&schema, &mut null_expr, &mut list, true, &record).unwrap(),
            Field::Null
        );

        let mut expr = Expression::Literal(Field::Int(3));
        let mut list_with_null = vec![
            Expression::Literal(Field::Int(1)),
            Expression::Literal(Field::Null),
        ];
        assert_eq!(
            evaluate_in_list(&schema, &mut expr, &mut list_with_null, false, &record).unwrap(),
            Field::Null
        );
        assert_eq!(
            evaluate_in_list(&schema, &mut expr, &mut list_with_null, true, &record).unwrap(),
            Field::Null
        );

        let mut matched_expr = Expression::Literal(Field::Int(1));
        assert_eq!(
            evaluate_in_list(&schema, &mut matched_expr, &mut list_with_null, false, &record)
                .unwrap(),
            Field::Boolean(true)
        );
        assert_eq!(
            evaluate_in_list(&schema, &mut matched_expr, &mut list_with_null, true, &record)
                .unwrap(),
            Field::Boolean(false)
        );
    }
}
