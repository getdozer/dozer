use crate::error::Error;
use crate::execution::{Expression, ExpressionType};
use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

pub(crate) fn evaluate_nvl(
    schema: &Schema,
    arg: &mut Expression,
    replacement: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let arg_field = arg.evaluate(record, schema)?;
    let replacement_field = replacement.evaluate(record, schema)?;
    if replacement_field.as_string().is_some() && arg_field == Field::Null {
        Ok(replacement_field)
    } else {
        Ok(arg_field)
    }
}

pub fn validate_decode(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    if args.len() < 3 {
        return Err(Error::InvalidNumberOfArguments {
            function_name: "decode".to_string(),
            expected: 3..usize::MAX,
            actual: args.len(),
        });
    }

    let ret_type = args[2].get_type(schema)?.return_type;

    Ok(ExpressionType::new(
        ret_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_decode(
    schema: &Schema,
    arg: &mut Expression,
    results: &mut [Expression],
    default: Option<Expression>,
    record: &Record,
) -> Result<Field, Error> {
    let arg_field = arg.evaluate(record, schema)?;

    for chunk in results.chunks_exact_mut(2) {
        let coded = &mut chunk[0].clone();
        let decoded = &mut chunk[1];
        let coded_field = coded.evaluate(record, schema)?;
        if coded_field == arg_field {
            return decoded.evaluate(record, schema);
        }
    }

    if let Some(mut default) = default {
        default.evaluate(record, schema)
    } else {
        Ok(Field::Null)
    }
}

#[cfg(test)]
mod tests {
    use dozer_types::types::{Field, Record, Schema};

    use crate::{execution::Expression, scalar::field::evaluate_decode};

    #[test]
    fn test_decode() {
        let row = Record::new(vec![]);
        let mut value = Box::new(Expression::Literal(Field::Int(2)));
        let mut results = [
            Expression::Literal(Field::Int(1)),
            Expression::Literal(Field::String("Southlake".to_owned())),
            Expression::Literal(Field::Int(2)),
            Expression::Literal(Field::String("San Francisco".to_owned())),
            Expression::Literal(Field::Int(3)),
            Expression::Literal(Field::String("New Jersey".to_owned())),
            Expression::Literal(Field::Int(4)),
            Expression::Literal(Field::String("Seattle".to_owned())),
        ];
        let default = Some(Expression::Literal(Field::String(
            "Non domestic".to_owned(),
        )));
        let result = Field::String("San Francisco".to_owned());

        assert_eq!(
            evaluate_decode(
                &Schema::default(),
                &mut value,
                &mut results,
                default.clone(),
                &row
            )
            .unwrap(),
            result
        );

        let mut value = Box::new(Expression::Literal(Field::Int(5)));
        let result = Field::String("Non domestic".to_owned());

        assert_eq!(
            evaluate_decode(&Schema::default(), &mut value, &mut results, default, &row).unwrap(),
            result
        );
    }
}
