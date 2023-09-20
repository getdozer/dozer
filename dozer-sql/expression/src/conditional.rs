use crate::error::Error;
use crate::execution::{Expression, ExpressionType};
use dozer_types::types::Record;
use dozer_types::types::{Field, FieldType, Schema};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ConditionalExpressionType {
    Coalesce,
    NullIf,
}

pub(crate) fn get_conditional_expr_type(
    function: &ConditionalExpressionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    match function {
        ConditionalExpressionType::Coalesce => validate_coalesce(args, schema),
        ConditionalExpressionType::NullIf => todo!(),
    }
}

impl ConditionalExpressionType {
    pub(crate) fn new(name: &str) -> Option<ConditionalExpressionType> {
        match name {
            "coalesce" => Some(ConditionalExpressionType::Coalesce),
            "nullif" => Some(ConditionalExpressionType::NullIf),
            _ => None,
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        args: &[Expression],
        record: &Record,
    ) -> Result<Field, Error> {
        match self {
            ConditionalExpressionType::Coalesce => evaluate_coalesce(schema, args, record),
            ConditionalExpressionType::NullIf => todo!(),
        }
    }
}

pub(crate) fn validate_coalesce(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    if args.is_empty() {
        return Err(Error::EmptyCoalesceArguments);
    }

    let return_types = args
        .iter()
        .map(|expr| expr.get_type(schema).unwrap().return_type)
        .collect::<Vec<FieldType>>();
    let return_type = return_types[0];

    Ok(ExpressionType::new(
        return_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_coalesce(
    schema: &Schema,
    args: &[Expression],
    record: &Record,
) -> Result<Field, Error> {
    // The COALESCE function returns the first of its arguments that is not null.
    for expr in args {
        let field = expr.evaluate(record, schema)?;
        if field != Field::Null {
            return Ok(field);
        }
    }
    // Null is returned only if all arguments are null.
    Ok(Field::Null)
}

impl Display for ConditionalExpressionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConditionalExpressionType::Coalesce => f.write_str("COALESCE"),
            ConditionalExpressionType::NullIf => f.write_str("NULLIF"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::{ArbitraryDateTime, ArbitraryDecimal};

    use super::*;

    use dozer_types::{
        ordered_float::OrderedFloat,
        types::{FieldDefinition, SourceDefinition},
    };
    use proptest::prelude::*;

    #[test]
    fn test_coalesce() {
        proptest!(ProptestConfig::with_cases(1000), move |(
            u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64,
            d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal,
            s_val1: String, s_val2: String,
            dt_val1: ArbitraryDateTime, dt_val2: ArbitraryDateTime)| {
            let uint1 = Expression::Literal(Field::UInt(u_num1));
            let uint2 = Expression::Literal(Field::UInt(u_num2));
            let int1 = Expression::Literal(Field::Int(i_num1));
            let int2 = Expression::Literal(Field::Int(i_num2));
            let float1 = Expression::Literal(Field::Float(OrderedFloat(f_num1)));
            let float2 = Expression::Literal(Field::Float(OrderedFloat(f_num2)));
            let dec1 = Expression::Literal(Field::Decimal(d_num1.0));
            let dec2 = Expression::Literal(Field::Decimal(d_num2.0));
            let str1 = Expression::Literal(Field::String(s_val1.clone()));
            let str2 = Expression::Literal(Field::String(s_val2));
            let t1 = Expression::Literal(Field::Timestamp(dt_val1.0));
            let t2 = Expression::Literal(Field::Timestamp(dt_val1.0));
            let dt1 = Expression::Literal(Field::Date(dt_val1.0.date_naive()));
            let dt2 = Expression::Literal(Field::Date(dt_val2.0.date_naive()));
            let null = Expression::Column{ index: 0usize };

            // UInt
            let typ = FieldType::UInt;
            let f = Field::UInt(u_num1);
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), uint1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), uint1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), uint1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), uint1, uint2];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // Int
            let typ = FieldType::Int;
            let f = Field::Int(i_num1);
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), int1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), int1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), int1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), int1, int2];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // Float
            let typ = FieldType::Float;
            let f = Field::Float(OrderedFloat(f_num1));
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), float1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), float1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), float1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), float1, float2];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // Decimal
            let typ = FieldType::Decimal;
            let f = Field::Decimal(d_num1.0);
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), dec1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), dec1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), dec1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), dec1, dec2];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // String
            let typ = FieldType::String;
            let f = Field::String(s_val1.clone());
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), str1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), str1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), str1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), str1.clone(), str2.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // String
            let typ = FieldType::String;
            let f = Field::String(s_val1);
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), str1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), str1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), str1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), str1, str2];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // Timestamp
            let typ = FieldType::Timestamp;
            let f = Field::Timestamp(dt_val1.0);
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), t1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), t1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), t1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), t1, t2];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // Date
            let typ = FieldType::Date;
            let f = Field::Date(dt_val1.0.date_naive());
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone(), dt1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), dt1.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), dt1.clone(), null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null.clone(), dt1, dt2];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);

            // Null
            let typ = FieldType::Date;
            let f = Field::Null;
            let row = Record::new(vec![f.clone()]);

            let args = vec![null.clone()];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f.clone());

            let args = vec![null.clone(), null];
            test_validate_coalesce(&args, typ);
            test_evaluate_coalesce(&args, &row, typ, f);
        });
    }

    fn test_validate_coalesce(args: &[Expression], typ: FieldType) {
        let schema = Schema::default()
            .field(
                FieldDefinition::new(String::from("field"), typ, false, SourceDefinition::Dynamic),
                false,
            )
            .clone();

        let result = validate_coalesce(args, &schema).unwrap().return_type;
        assert_eq!(result, typ);
    }

    fn test_evaluate_coalesce(args: &[Expression], row: &Record, typ: FieldType, _result: Field) {
        let schema = Schema::default()
            .field(
                FieldDefinition::new(String::from("field"), typ, false, SourceDefinition::Dynamic),
                false,
            )
            .clone();

        let res = evaluate_coalesce(&schema, args, row).unwrap();
        assert_eq!(res, _result);
    }
}
