use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

use crate::error::Error;
use crate::execution::Expression;

pub(crate) fn evaluate_is_null(
    schema: &Schema,
    expr: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let field = expr.evaluate(record, schema)?;
    Ok(Field::Boolean(field == Field::Null))
}

pub(crate) fn evaluate_is_not_null(
    schema: &Schema,
    expr: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let field = expr.evaluate(record, schema)?;
    Ok(Field::Boolean(field != Field::Null))
}

#[test]
fn test_is_null() {
    let mut value = Box::new(Expression::Literal(Field::Int(65)));
    assert_eq!(
        evaluate_is_null(&Schema::default(), &mut value, &Record::new(vec![])).unwrap(),
        Field::Boolean(false)
    );

    let mut value = Box::new(Expression::Literal(Field::Null));
    assert_eq!(
        evaluate_is_null(&Schema::default(), &mut value, &Record::new(vec![])).unwrap(),
        Field::Boolean(true)
    );

    let mut value = Box::new(Expression::Literal(Field::Int(65)));
    assert_eq!(
        evaluate_is_not_null(&Schema::default(), &mut value, &Record::new(vec![])).unwrap(),
        Field::Boolean(true)
    );

    let mut value = Box::new(Expression::Literal(Field::Null));
    assert_eq!(
        evaluate_is_not_null(&Schema::default(), &mut value, &Record::new(vec![])).unwrap(),
        Field::Boolean(false)
    );
}
