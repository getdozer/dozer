use crate::pipeline::expression::comparison::{
    evaluate_eq, evaluate_gt, evaluate_gte, evaluate_lt, evaluate_lte, evaluate_ne, DATE_FORMAT,
};
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::tests::test_common::*;
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::chrono::{DateTime, NaiveDate};
use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};
use dozer_types::{
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    types::{Field, Schema},
};
use num_traits::FromPrimitive;
use proptest::prelude::*;

#[test]
fn test_comparison() {
    proptest!(ProptestConfig::with_cases(1000), move |(
        u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = ProcessorRecord::new();

        let uint1 = Literal(Field::UInt(u_num1));
        let uint2 = Literal(Field::UInt(u_num2));
        let int1 = Literal(Field::Int(i_num1));
        let int2 = Literal(Field::Int(i_num2));
        let float1 = Literal(Field::Float(OrderedFloat(f_num1)));
        let float2 = Literal(Field::Float(OrderedFloat(f_num2)));
        let dec1 = Literal(Field::Decimal(d_num1.0));
        let dec2 = Literal(Field::Decimal(d_num2.0));
        let null = Literal(Field::Null);

        // eq: UInt
        test_eq(&uint1, &uint1, &row, None);
        if u_num1 == u_num2 && u_num1 as i64 == i_num1 && u_num1 as f64 == f_num1 && Decimal::from(u_num1) == d_num1.0 {
            test_eq(&uint1, &uint2, &row, None);
            test_eq(&uint1, &int1, &row, None);
            test_eq(&uint1, &float1, &row, None);
            test_eq(&uint1, &dec1, &row, None);
            test_eq(&uint1, &null, &row, Some(Field::Null));

            test_gte(&uint1, &uint2, &row, None);
            test_gte(&uint1, &int1, &row, None);
            test_gte(&uint1, &float1, &row, None);
            test_gte(&uint1, &dec1, &row, None);
            test_gte(&uint1, &null, &row, Some(Field::Null));

            test_lte(&uint1, &uint2, &row, None);
            test_lte(&uint1, &int1, &row, None);
            test_lte(&uint1, &float1, &row, None);
            test_lte(&uint1, &dec1, &row, None);
            test_lte(&uint1, &null, &row, Some(Field::Null));
        }

        // eq: Int
        test_eq(&int1, &int1, &row, None);
        if i_num1 == u_num1 as i64 && i_num1 == i_num2 && i_num1 as f64 == f_num1 && Decimal::from(i_num1) == d_num1.0 {
            test_eq(&int1, &uint1, &row, None);
            test_eq(&int1, &int2, &row, None);
            test_eq(&int1, &float1, &row, None);
            test_eq(&int1, &dec1, &row, None);
            test_eq(&int1, &null, &row, Some(Field::Null));

            test_gte(&int1, &uint1, &row, None);
            test_gte(&int1, &int2, &row, None);
            test_gte(&int1, &float1, &row, None);
            test_gte(&int1, &dec1, &row, None);
            test_gte(&int1, &null, &row, Some(Field::Null));

            test_lte(&int1, &uint1, &row, None);
            test_lte(&int1, &int2, &row, None);
            test_lte(&int1, &float1, &row, None);
            test_lte(&int1, &dec1, &row, None);
            test_lte(&int1, &null, &row, Some(Field::Null));
        }

        // eq: Float
        test_eq(&float1, &float1, &row, None);
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 == u_num1 as f64 && f_num1 == i_num1 as f64 && f_num1 == f_num2 && d_val.unwrap() == d_num1.0 {
            test_eq(&float1, &uint1, &row, None);
            test_eq(&float1, &int1, &row, None);
            test_eq(&float1, &float2, &row, None);
            test_eq(&float1, &dec1, &row, None);
            test_eq(&float1, &null, &row, Some(Field::Null));

            test_gte(&float1, &uint1, &row, None);
            test_gte(&float1, &int1, &row, None);
            test_gte(&float1, &float2, &row, None);
            test_gte(&float1, &dec1, &row, None);
            test_gte(&float1, &null, &row, Some(Field::Null));

            test_lte(&float1, &uint1, &row, None);
            test_lte(&float1, &int1, &row, None);
            test_lte(&float1, &float2, &row, None);
            test_lte(&float1, &dec1, &row, None);
            test_lte(&float1, &null, &row, Some(Field::Null));
        }

        // eq: Decimal
        test_eq(&dec1, &dec1, &row, None);
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 == Decimal::from(u_num1) && d_num1.0 == Decimal::from(i_num1) && d_num1.0 == d_val.unwrap() && d_num1.0 == d_num2.0 {
            test_eq(&dec1, &uint1, &row, None);
            test_eq(&dec1, &int1, &row, None);
            test_eq(&dec1, &float1, &row, None);
            test_eq(&dec1, &dec2, &row, None);
            test_eq(&dec1, &null, &row, Some(Field::Null));

            test_gte(&dec1, &uint1, &row, None);
            test_gte(&dec1, &int1, &row, None);
            test_gte(&dec1, &float1, &row, None);
            test_gte(&dec1, &dec2, &row, None);
            test_gte(&dec1, &null, &row, Some(Field::Null));

            test_lte(&dec1, &uint1, &row, None);
            test_lte(&dec1, &int1, &row, None);
            test_lte(&dec1, &float1, &row, None);
            test_lte(&dec1, &dec2, &row, None);
            test_lte(&dec1, &null, &row, Some(Field::Null));
        }

        // eq: Null
        test_eq(&null, &uint2, &row, Some(Field::Null));
        test_eq(&null, &int2, &row, Some(Field::Null));
        test_eq(&null, &float2, &row, Some(Field::Null));
        test_eq(&null, &dec2, &row, Some(Field::Null));
        test_eq(&null, &null, &row, Some(Field::Null));

        // not eq: UInt
        if u_num1 != u_num2 && u_num1 as i64 != i_num1 && u_num1 as f64 != f_num1 && Decimal::from(u_num1) != d_num1.0 {
            test_eq(&uint1, &uint2, &row, Some(Field::Boolean(false)));
            test_eq(&uint1, &int1, &row, Some(Field::Boolean(false)));
            test_eq(&uint1, &float1, &row, Some(Field::Boolean(false)));
            test_eq(&uint1, &dec1, &row, Some(Field::Boolean(false)));
            test_eq(&uint1, &null, &row, Some(Field::Null));

            test_ne(&uint1, &uint2, &row, None);
            test_ne(&uint1, &int1, &row, None);
            test_ne(&uint1, &float1, &row, None);
            test_ne(&uint1, &dec1, &row, None);
            test_ne(&uint1, &null, &row, Some(Field::Null));
        }

        // not eq: Int
        if i_num1 != u_num1 as i64 && i_num1 != i_num2 && i_num1 as f64 != f_num1 && Decimal::from(i_num1) != d_num1.0 {
            test_eq(&int1, &uint1, &row, Some(Field::Boolean(false)));
            test_eq(&int1, &int2, &row, Some(Field::Boolean(false)));
            test_eq(&int1, &float1, &row, Some(Field::Boolean(false)));
            test_eq(&int1, &dec1, &row, Some(Field::Boolean(false)));
            test_eq(&int1, &null, &row, Some(Field::Null));

            test_ne(&int1, &uint1, &row, None);
            test_ne(&int1, &int2, &row, None);
            test_ne(&int1, &float1, &row, None);
            test_ne(&int1, &dec1, &row, None);
            test_ne(&int1, &null, &row, Some(Field::Null));
        }

        // not eq: Float
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 != u_num1 as f64 && f_num1 != i_num1 as f64 && f_num1 != f_num2 && d_val.unwrap() != d_num1.0 {
            test_eq(&float1, &uint1, &row, Some(Field::Boolean(false)));
            test_eq(&float1, &int1, &row, Some(Field::Boolean(false)));
            test_eq(&float1, &float2, &row, Some(Field::Boolean(false)));
            test_eq(&float1, &dec1, &row, Some(Field::Boolean(false)));
            test_eq(&float1, &null, &row, Some(Field::Null));

            test_ne(&float1, &uint1, &row, None);
            test_ne(&float1, &int1, &row, None);
            test_ne(&float1, &float2, &row, None);
            test_ne(&float1, &dec1, &row, None);
            test_ne(&float1, &null, &row, Some(Field::Null));
        }

        // not eq: Decimal
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 != Decimal::from(u_num1) && d_num1.0 != Decimal::from(i_num1) && d_num1.0 != d_val.unwrap() && d_num1.0 != d_num2.0 {
            test_eq(&dec1, &uint1, &row, Some(Field::Boolean(false)));
            test_eq(&dec1, &int1, &row, Some(Field::Boolean(false)));
            test_eq(&dec1, &float1, &row, Some(Field::Boolean(false)));
            test_eq(&dec1, &dec2, &row, Some(Field::Boolean(false)));
            test_eq(&dec1, &null, &row, Some(Field::Null));

            test_ne(&dec1, &uint1, &row, None);
            test_ne(&dec1, &int1, &row, None);
            test_ne(&dec1, &float1, &row, None);
            test_ne(&dec1, &dec2, &row, None);
            test_ne(&dec1, &null, &row, Some(Field::Null));
        }

        // not eq: Null
        test_eq(&null, &uint2, &row, Some(Field::Null));
        test_eq(&null, &int2, &row, Some(Field::Null));
        test_eq(&null, &float2, &row, Some(Field::Null));
        test_eq(&null, &dec2, &row, Some(Field::Null));

        test_ne(&null, &uint2, &row, Some(Field::Null));
        test_ne(&null, &int2, &row, Some(Field::Null));
        test_ne(&null, &float2, &row, Some(Field::Null));
        test_ne(&null, &dec2, &row, Some(Field::Null));

        // gt: UInt
        if u_num1 > u_num2 && u_num1 as i64 > i_num1 && u_num1 as f64 > f_num1 && Decimal::from(u_num1) > d_num1.0 {
            test_gt(&uint1, &uint2, &row, None);
            test_gt(&uint1, &int1, &row, None);
            test_gt(&uint1, &float1, &row, None);
            test_gt(&uint1, &dec1, &row, None);
            test_gt(&uint1, &null, &row, Some(Field::Null));

            test_gte(&uint1, &uint2, &row, None);
            test_gte(&uint1, &int1, &row, None);
            test_gte(&uint1, &float1, &row, None);
            test_gte(&uint1, &dec1, &row, None);
            test_gte(&uint1, &null, &row, Some(Field::Null));

            test_lt(&uint1, &uint2, &row, Some(Field::Boolean(false)));
            test_lt(&uint1, &int1, &row, Some(Field::Boolean(false)));
            test_lt(&uint1, &float1, &row, Some(Field::Boolean(false)));
            test_lt(&uint1, &dec1, &row, Some(Field::Boolean(false)));
            test_lt(&uint1, &null, &row, Some(Field::Null));

            test_lte(&uint1, &uint2, &row, Some(Field::Boolean(false)));
            test_lte(&uint1, &int1, &row, Some(Field::Boolean(false)));
            test_lte(&uint1, &float1, &row, Some(Field::Boolean(false)));
            test_lte(&uint1, &dec1, &row, Some(Field::Boolean(false)));
            test_lte(&uint1, &null, &row, Some(Field::Null));
        }

        // gt: Int
        if i_num1 > u_num1 as i64 && i_num1 > i_num2 && i_num1 as f64 > f_num1 && Decimal::from(i_num1) > d_num1.0 {
            test_gt(&int1, &uint1, &row, None);
            test_gt(&int1, &int2, &row, None);
            test_gt(&int1, &float1, &row, None);
            test_gt(&int1, &dec1, &row, None);
            test_gt(&int1, &null, &row, Some(Field::Null));

            test_gte(&int1, &uint1, &row, None);
            test_gte(&int1, &int2, &row, None);
            test_gte(&int1, &float1, &row, None);
            test_gte(&int1, &dec1, &row, None);
            test_gte(&int1, &null, &row, Some(Field::Null));

            test_lt(&int1, &uint1, &row, Some(Field::Boolean(false)));
            test_lt(&int1, &int2, &row, Some(Field::Boolean(false)));
            test_lt(&int1, &float1, &row, Some(Field::Boolean(false)));
            test_lt(&int1, &dec1, &row, Some(Field::Boolean(false)));
            test_lt(&int1, &null, &row, Some(Field::Null));

            test_lte(&int1, &uint1, &row, Some(Field::Boolean(false)));
            test_lte(&int1, &int2, &row, Some(Field::Boolean(false)));
            test_lte(&int1, &float1, &row, Some(Field::Boolean(false)));
            test_lte(&int1, &dec1, &row, Some(Field::Boolean(false)));
            test_lte(&int1, &null, &row, Some(Field::Null));
        }

        // gt: Float
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 > u_num1 as f64 && f_num1 > i_num1 as f64 && f_num1 > f_num2 && d_val.unwrap() > d_num1.0 {
            test_gt(&float1, &uint1, &row, None);
            test_gt(&float1, &int1, &row, None);
            test_gt(&float1, &float2, &row, None);
            test_gt(&float1, &dec1, &row, None);
            test_gt(&float1, &null, &row, Some(Field::Null));

            test_gte(&float1, &uint1, &row, None);
            test_gte(&float1, &int1, &row, None);
            test_gte(&float1, &float2, &row, None);
            test_gte(&float1, &dec1, &row, None);
            test_gte(&float1, &null, &row, Some(Field::Null));

            test_lt(&float1, &uint1, &row, Some(Field::Boolean(false)));
            test_lt(&float1, &int1, &row, Some(Field::Boolean(false)));
            test_lt(&float1, &float2, &row, Some(Field::Boolean(false)));
            test_lt(&float1, &dec1, &row, Some(Field::Boolean(false)));
            test_lt(&float1, &null, &row, Some(Field::Null));

            test_lte(&float1, &uint1, &row, Some(Field::Boolean(false)));
            test_lte(&float1, &int1, &row, Some(Field::Boolean(false)));
            test_lte(&float1, &float2, &row, Some(Field::Boolean(false)));
            test_lte(&float1, &dec1, &row, Some(Field::Boolean(false)));
            test_lte(&float1, &null, &row, Some(Field::Null));
        }

        // gt: Decimal
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 > Decimal::from(u_num1) && d_num1.0 > Decimal::from(i_num1) && d_num1.0 > d_val.unwrap() && d_num1.0 > d_num2.0 {
            test_gt(&dec1, &uint1, &row, None);
            test_gt(&dec1, &int1, &row, None);
            test_gt(&dec1, &float1, &row, None);
            test_gt(&dec1, &dec2, &row, None);
            test_gt(&dec1, &null, &row, Some(Field::Null));

            test_gte(&dec1, &uint1, &row, None);
            test_gte(&dec1, &int1, &row, None);
            test_gte(&dec1, &float1, &row, None);
            test_gte(&dec1, &dec2, &row, None);
            test_gte(&dec1, &null, &row, Some(Field::Null));

            test_lt(&dec1, &uint1, &row, Some(Field::Boolean(false)));
            test_lt(&dec1, &int1, &row, Some(Field::Boolean(false)));
            test_lt(&dec1, &float1, &row, Some(Field::Boolean(false)));
            test_lt(&dec1, &dec2, &row, Some(Field::Boolean(false)));
            test_lt(&dec1, &null, &row, Some(Field::Null));

            test_lte(&dec1, &uint1, &row, Some(Field::Boolean(false)));
            test_lte(&dec1, &int1, &row, Some(Field::Boolean(false)));
            test_lt(&dec1, &float1, &row, Some(Field::Boolean(false)));
            test_lte(&dec1, &dec2, &row, Some(Field::Boolean(false)));
            test_lte(&dec1, &null, &row, Some(Field::Null));
        }

        // lt: UInt
        if u_num1 < u_num2 && (u_num1 as i64) < i_num1 && (u_num1 as f64) < f_num1 && Decimal::from(u_num1) < d_num1.0 {
            test_lt(&uint1, &uint2, &row, None);
            test_lt(&uint1, &int1, &row, None);
            test_lt(&uint1, &float1, &row, None);
            test_lt(&uint1, &dec1, &row, None);
            test_lt(&uint1, &null, &row, Some(Field::Null));

            test_lte(&uint1, &uint2, &row, None);
            test_lte(&uint1, &int1, &row, None);
            test_lte(&uint1, &float1, &row, None);
            test_lte(&uint1, &dec1, &row, None);
            test_lte(&uint1, &null, &row, Some(Field::Null));

            test_gt(&uint1, &uint2, &row, Some(Field::Boolean(false)));
            test_gt(&uint1, &int1, &row, Some(Field::Boolean(false)));
            test_gt(&uint1, &float1, &row, Some(Field::Boolean(false)));
            test_gt(&uint1, &dec1, &row, Some(Field::Boolean(false)));
            test_gt(&uint1, &null, &row, Some(Field::Null));

            test_gte(&uint1, &uint2, &row, Some(Field::Boolean(false)));
            test_gte(&uint1, &int1, &row, Some(Field::Boolean(false)));
            test_gte(&uint1, &float1, &row, Some(Field::Boolean(false)));
            test_gte(&uint1, &dec1, &row, Some(Field::Boolean(false)));
            test_gte(&uint1, &null, &row, Some(Field::Null));
        }

        // gt: Int
        if i_num1 < (u_num1 as i64) && i_num1 < i_num2 && (i_num1 as f64) < f_num1 && Decimal::from(i_num1) < d_num1.0 {
            test_lt(&int1, &uint1, &row, None);
            test_lt(&int1, &int2, &row, None);
            test_lt(&int1, &float1, &row, None);
            test_lt(&int1, &dec1, &row, None);
            test_lt(&int1, &null, &row, Some(Field::Null));

            test_lte(&int1, &uint1, &row, None);
            test_lte(&int1, &int2, &row, None);
            test_lte(&int1, &float1, &row, None);
            test_lte(&int1, &dec1, &row, None);
            test_lte(&int1, &null, &row, Some(Field::Null));

            test_gt(&int1, &uint1, &row, Some(Field::Boolean(false)));
            test_gt(&int1, &int2, &row, Some(Field::Boolean(false)));
            test_gt(&int1, &float1, &row, Some(Field::Boolean(false)));
            test_gt(&int1, &dec1, &row, Some(Field::Boolean(false)));
            test_gt(&int1, &null, &row, Some(Field::Null));

            test_gte(&int1, &uint1, &row, Some(Field::Boolean(false)));
            test_gte(&int1, &int2, &row, Some(Field::Boolean(false)));
            test_gte(&int1, &float1, &row, Some(Field::Boolean(false)));
            test_gte(&int1, &dec1, &row, Some(Field::Boolean(false)));
            test_gte(&int1, &null, &row, Some(Field::Null));
        }

        // gt: Float
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 < u_num1 as f64 && f_num1 < i_num1 as f64 && f_num1 < f_num2 && d_val.unwrap() < d_num1.0 {
            test_lt(&float1, &uint1, &row, None);
            test_lt(&float1, &int1, &row, None);
            test_lt(&float1, &float2, &row, None);
            test_lt(&float1, &dec1, &row, None);
            test_lt(&float1, &null, &row, Some(Field::Null));

            test_lte(&float1, &uint1, &row, None);
            test_lte(&float1, &int1, &row, None);
            test_lte(&float1, &float2, &row, None);
            test_lte(&float1, &dec1, &row, None);
            test_lte(&float1, &null, &row, Some(Field::Null));

            test_gt(&float1, &uint1, &row, Some(Field::Boolean(false)));
            test_gt(&float1, &int1, &row, Some(Field::Boolean(false)));
            test_gt(&float1, &float2, &row, Some(Field::Boolean(false)));
            test_gt(&float1, &dec1, &row, Some(Field::Boolean(false)));
            test_gt(&float1, &null, &row, Some(Field::Null));

            test_gte(&float1, &uint1, &row, Some(Field::Boolean(false)));
            test_gte(&float1, &int1, &row, Some(Field::Boolean(false)));
            test_gte(&float1, &float2, &row, Some(Field::Boolean(false)));
            test_gte(&float1, &dec1, &row, Some(Field::Boolean(false)));
            test_gte(&float1, &null, &row, Some(Field::Null));
        }

        // gt: Decimal
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 < Decimal::from(u_num1) && d_num1.0 < Decimal::from(i_num1) && d_num1.0 < d_val.unwrap() && d_num1.0 < d_num2.0 {
            test_lt(&dec1, &uint1, &row, None);
            test_lt(&dec1, &int1, &row, None);
            test_lt(&dec1, &float1, &row, None);
            test_lt(&dec1, &dec2, &row, None);
            test_lt(&dec1, &null, &row, Some(Field::Null));

            test_lte(&dec1, &uint1, &row, None);
            test_lte(&dec1, &int1, &row, None);
            test_lte(&dec1, &float1, &row, None);
            test_lte(&dec1, &dec2, &row, None);
            test_lte(&dec1, &null, &row, Some(Field::Null));

            test_gt(&dec1, &uint1, &row, Some(Field::Boolean(false)));
            test_gt(&dec1, &int1, &row, Some(Field::Boolean(false)));
            test_gt(&dec1, &float1, &row, Some(Field::Boolean(false)));
            test_gt(&dec1, &dec2, &row, Some(Field::Boolean(false)));
            test_gt(&dec1, &null, &row, Some(Field::Null));

            test_gte(&dec1, &uint1, &row, Some(Field::Boolean(false)));
            test_gte(&dec1, &int1, &row, Some(Field::Boolean(false)));
            test_gte(&dec1, &float1, &row, Some(Field::Boolean(false)));
            test_gte(&dec1, &dec2, &row, Some(Field::Boolean(false)));
            test_gte(&dec1, &null, &row, Some(Field::Null));
        }
    });
}

fn test_eq(exp1: &Expression, exp2: &Expression, row: &ProcessorRecord, result: Option<Field>) {
    match result {
        None => {
            assert!(matches!(
                evaluate_eq(&Schema::default(), exp1, exp2, row),
                Ok(Field::Boolean(true))
            ));
        }
        Some(_val) => {
            assert!(matches!(
                evaluate_eq(&Schema::default(), exp1, exp2, row),
                Ok(_val)
            ));
        }
    }
}

fn test_ne(exp1: &Expression, exp2: &Expression, row: &ProcessorRecord, result: Option<Field>) {
    match result {
        None => {
            assert!(matches!(
                evaluate_ne(&Schema::default(), exp1, exp2, row),
                Ok(Field::Boolean(true))
            ));
        }
        Some(_val) => {
            assert!(matches!(
                evaluate_ne(&Schema::default(), exp1, exp2, row),
                Ok(_val)
            ));
        }
    }
}

fn test_gt(exp1: &Expression, exp2: &Expression, row: &ProcessorRecord, result: Option<Field>) {
    match result {
        None => {
            assert!(matches!(
                evaluate_gt(&Schema::default(), exp1, exp2, row),
                Ok(Field::Boolean(true))
            ));
        }
        Some(_val) => {
            assert!(matches!(
                evaluate_gt(&Schema::default(), exp1, exp2, row),
                Ok(_val)
            ));
        }
    }
}

fn test_lt(exp1: &Expression, exp2: &Expression, row: &ProcessorRecord, result: Option<Field>) {
    match result {
        None => {
            assert!(matches!(
                evaluate_lt(&Schema::default(), exp1, exp2, row),
                Ok(Field::Boolean(true))
            ));
        }
        Some(_val) => {
            assert!(matches!(
                evaluate_lt(&Schema::default(), exp1, exp2, row),
                Ok(_val)
            ));
        }
    }
}

fn test_gte(exp1: &Expression, exp2: &Expression, row: &ProcessorRecord, result: Option<Field>) {
    match result {
        None => {
            assert!(matches!(
                evaluate_gte(&Schema::default(), exp1, exp2, row),
                Ok(Field::Boolean(true))
            ));
        }
        Some(_val) => {
            assert!(matches!(
                evaluate_gte(&Schema::default(), exp1, exp2, row),
                Ok(_val)
            ));
        }
    }
}

fn test_lte(exp1: &Expression, exp2: &Expression, row: &ProcessorRecord, result: Option<Field>) {
    match result {
        None => {
            assert!(matches!(
                evaluate_lte(&Schema::default(), exp1, exp2, row),
                Ok(Field::Boolean(true))
            ));
        }
        Some(_val) => {
            assert!(matches!(
                evaluate_lte(&Schema::default(), exp1, exp2, row),
                Ok(_val)
            ));
        }
    }
}

#[test]
fn test_comparison_logical_int() {
    let record = vec![Field::Int(124)];
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                String::from("id"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let f = run_fct(
        "SELECT id FROM users WHERE id = '124'",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Int(124));

    let f = run_fct(
        "SELECT id FROM users WHERE id <= '124'",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Int(124));

    let f = run_fct(
        "SELECT id FROM users WHERE id >= '124'",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Int(124));

    let f = run_fct(
        "SELECT id = '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT id < '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT id > '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT id <= '124' FROM users",
        schema.clone(),
        record.clone(),
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct("SELECT id >= '124' FROM users", schema, record);
    assert_eq!(f, Field::Boolean(true));
}

#[test]
fn test_comparison_logical_timestamp() {
    let f = run_fct(
        "SELECT time = '2020-01-01T00:00:00Z' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("time"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT time < '2020-01-01T00:00:01Z' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("time"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Timestamp(
            DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z").unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));
}

#[test]
fn test_comparison_logical_date() {
    let f = run_fct(
        "SELECT date = '2020-01-01' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("date"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(
            NaiveDate::parse_from_str("2020-01-01", DATE_FORMAT).unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT date != '2020-01-01' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("date"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(
            NaiveDate::parse_from_str("2020-01-01", DATE_FORMAT).unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT date > '2020-01-01' FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("date"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Date(
            NaiveDate::parse_from_str("2020-01-02", DATE_FORMAT).unwrap(),
        )],
    );
    assert_eq!(f, Field::Boolean(true));
}
