use crate::tests::ArbitraryDecimal;

use super::*;

use dozer_types::{ordered_float::OrderedFloat, rust_decimal::Decimal};
use num_traits::FromPrimitive;
use proptest::prelude::*;
use Expression::Literal;

#[test]
fn test_comparison() {
    proptest!(ProptestConfig::with_cases(1000), move |(
        u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Literal(Field::UInt(u_num1));
        let mut uint2 = Literal(Field::UInt(u_num2));
        let mut int1 = Literal(Field::Int(i_num1));
        let mut int2 = Literal(Field::Int(i_num2));
        let mut float1 = Literal(Field::Float(OrderedFloat(f_num1)));
        let mut float2 = Literal(Field::Float(OrderedFloat(f_num2)));
        let mut dec1 = Literal(Field::Decimal(d_num1.0));
        let mut dec2 = Literal(Field::Decimal(d_num2.0));
        let mut null = Literal(Field::Null);

        // eq: UInt
        let mut uint1_clone = uint1.clone();
        test_eq(&mut uint1, &mut uint1_clone, &row, None);
        if u_num1 == u_num2 && u_num1 as i64 == i_num1 && u_num1 as f64 == f_num1 && Decimal::from(u_num1) == d_num1.0 {
            test_eq(&mut uint1, &mut uint2, &row, None);
            test_eq(&mut uint1, &mut int1, &row, None);
            test_eq(&mut uint1, &mut float1, &row, None);
            test_eq(&mut uint1, &mut dec1, &row, None);
            test_eq(&mut uint1, &mut null, &row, Some(Field::Null));

            test_gte(&mut uint1, &mut uint2, &row, None);
            test_gte(&mut uint1, &mut int1, &row, None);
            test_gte(&mut uint1, &mut float1, &row, None);
            test_gte(&mut uint1, &mut dec1, &row, None);
            test_gte(&mut uint1, &mut null, &row, Some(Field::Null));

            test_lte(&mut uint1, &mut uint2, &row, None);
            test_lte(&mut uint1, &mut int1, &row, None);
            test_lte(&mut uint1, &mut float1, &row, None);
            test_lte(&mut uint1, &mut dec1, &row, None);
            test_lte(&mut uint1, &mut null, &row, Some(Field::Null));
        }

        // eq: Int
        let mut int1_clone = int1.clone();
        test_eq(&mut int1, &mut int1_clone, &row, None);
        if i_num1 == u_num1 as i64 && i_num1 == i_num2 && i_num1 as f64 == f_num1 && Decimal::from(i_num1) == d_num1.0 {
            test_eq(&mut int1, &mut uint1, &row, None);
            test_eq(&mut int1, &mut int2, &row, None);
            test_eq(&mut int1, &mut float1, &row, None);
            test_eq(&mut int1, &mut dec1, &row, None);
            test_eq(&mut int1, &mut null, &row, Some(Field::Null));

            test_gte(&mut int1, &mut uint1, &row, None);
            test_gte(&mut int1, &mut int2, &row, None);
            test_gte(&mut int1, &mut float1, &row, None);
            test_gte(&mut int1, &mut dec1, &row, None);
            test_gte(&mut int1, &mut null, &row, Some(Field::Null));

            test_lte(&mut int1, &mut uint1, &row, None);
            test_lte(&mut int1, &mut int2, &row, None);
            test_lte(&mut int1, &mut float1, &row, None);
            test_lte(&mut int1, &mut dec1, &row, None);
            test_lte(&mut int1, &mut null, &row, Some(Field::Null));
        }

        // eq: Float
        let mut float1_clone = float1.clone();
        test_eq(&mut float1, &mut float1_clone, &row, None);
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 == u_num1 as f64 && f_num1 == i_num1 as f64 && f_num1 == f_num2 && d_val.unwrap() == d_num1.0 {
            test_eq(&mut float1, &mut uint1, &row, None);
            test_eq(&mut float1, &mut int1, &row, None);
            test_eq(&mut float1, &mut float2, &row, None);
            test_eq(&mut float1, &mut dec1, &row, None);
            test_eq(&mut float1, &mut null, &row, Some(Field::Null));

            test_gte(&mut float1, &mut uint1, &row, None);
            test_gte(&mut float1, &mut int1, &row, None);
            test_gte(&mut float1, &mut float2, &row, None);
            test_gte(&mut float1, &mut dec1, &row, None);
            test_gte(&mut float1, &mut null, &row, Some(Field::Null));

            test_lte(&mut float1, &mut uint1, &row, None);
            test_lte(&mut float1, &mut int1, &row, None);
            test_lte(&mut float1, &mut float2, &row, None);
            test_lte(&mut float1, &mut dec1, &row, None);
            test_lte(&mut float1, &mut null, &row, Some(Field::Null));
        }

        // eq: Decimal
        let mut dec1_clone = dec1.clone();
        test_eq(&mut dec1, &mut dec1_clone, &row, None);
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 == Decimal::from(u_num1) && d_num1.0 == Decimal::from(i_num1) && d_num1.0 == d_val.unwrap() && d_num1.0 == d_num2.0 {
            test_eq(&mut dec1, &mut uint1, &row, None);
            test_eq(&mut dec1, &mut int1, &row, None);
            test_eq(&mut dec1, &mut float1, &row, None);
            test_eq(&mut dec1, &mut dec2, &row, None);
            test_eq(&mut dec1, &mut null, &row, Some(Field::Null));

            test_gte(&mut dec1, &mut uint1, &row, None);
            test_gte(&mut dec1, &mut int1, &row, None);
            test_gte(&mut dec1, &mut float1, &row, None);
            test_gte(&mut dec1, &mut dec2, &row, None);
            test_gte(&mut dec1, &mut null, &row, Some(Field::Null));

            test_lte(&mut dec1, &mut uint1, &row, None);
            test_lte(&mut dec1, &mut int1, &row, None);
            test_lte(&mut dec1, &mut float1, &row, None);
            test_lte(&mut dec1, &mut dec2, &row, None);
            test_lte(&mut dec1, &mut null, &row, Some(Field::Null));
        }

        // eq: Null
        test_eq(&mut null, &mut uint2, &row, Some(Field::Null));
        test_eq(&mut null, &mut int2, &row, Some(Field::Null));
        test_eq(&mut null, &mut float2, &row, Some(Field::Null));
        test_eq(&mut null, &mut dec2, &row, Some(Field::Null));
        let mut null_clone = null.clone();
        test_eq(&mut null, &mut null_clone, &row, Some(Field::Null));

        // not eq: UInt
        if u_num1 != u_num2 && u_num1 as i64 != i_num1 && u_num1 as f64 != f_num1 && Decimal::from(u_num1) != d_num1.0 {
            test_eq(&mut uint1, &mut uint2, &row, Some(Field::Boolean(false)));
            test_eq(&mut uint1, &mut int1, &row, Some(Field::Boolean(false)));
            test_eq(&mut uint1, &mut float1, &row, Some(Field::Boolean(false)));
            test_eq(&mut uint1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_eq(&mut uint1, &mut null, &row, Some(Field::Null));

            test_ne(&mut uint1, &mut uint2, &row, None);
            test_ne(&mut uint1, &mut int1, &row, None);
            test_ne(&mut uint1, &mut float1, &row, None);
            test_ne(&mut uint1, &mut dec1, &row, None);
            test_ne(&mut uint1, &mut null, &row, Some(Field::Null));
        }

        // not eq: Int
        if i_num1 != u_num1 as i64 && i_num1 != i_num2 && i_num1 as f64 != f_num1 && Decimal::from(i_num1) != d_num1.0 {
            test_eq(&mut int1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_eq(&mut int1, &mut int2, &row, Some(Field::Boolean(false)));
            test_eq(&mut int1, &mut float1, &row, Some(Field::Boolean(false)));
            test_eq(&mut int1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_eq(&mut int1, &mut null, &row, Some(Field::Null));

            test_ne(&mut int1, &mut uint1, &row, None);
            test_ne(&mut int1, &mut int2, &row, None);
            test_ne(&mut int1, &mut float1, &row, None);
            test_ne(&mut int1, &mut dec1, &row, None);
            test_ne(&mut int1, &mut null, &row, Some(Field::Null));
        }

        // not eq: Float
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 != u_num1 as f64 && f_num1 != i_num1 as f64 && f_num1 != f_num2 && d_val.unwrap() != d_num1.0 {
            test_eq(&mut float1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_eq(&mut float1, &mut int1, &row, Some(Field::Boolean(false)));
            test_eq(&mut float1, &mut float2, &row, Some(Field::Boolean(false)));
            test_eq(&mut float1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_eq(&mut float1, &mut null, &row, Some(Field::Null));

            test_ne(&mut float1, &mut uint1, &row, None);
            test_ne(&mut float1, &mut int1, &row, None);
            test_ne(&mut float1, &mut float2, &row, None);
            test_ne(&mut float1, &mut dec1, &row, None);
            test_ne(&mut float1, &mut null, &row, Some(Field::Null));
        }

        // not eq: Decimal
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 != Decimal::from(u_num1) && d_num1.0 != Decimal::from(i_num1) && d_num1.0 != d_val.unwrap() && d_num1.0 != d_num2.0 {
            test_eq(&mut dec1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_eq(&mut dec1, &mut int1, &row, Some(Field::Boolean(false)));
            test_eq(&mut dec1, &mut float1, &row, Some(Field::Boolean(false)));
            test_eq(&mut dec1, &mut dec2, &row, Some(Field::Boolean(false)));
            test_eq(&mut dec1, &mut null, &row, Some(Field::Null));

            test_ne(&mut dec1, &mut uint1, &row, None);
            test_ne(&mut dec1, &mut int1, &row, None);
            test_ne(&mut dec1, &mut float1, &row, None);
            test_ne(&mut dec1, &mut dec2, &row, None);
            test_ne(&mut dec1, &mut null, &row, Some(Field::Null));
        }

        // not eq: Null
        test_eq(&mut null, &mut uint2, &row, Some(Field::Null));
        test_eq(&mut null, &mut int2, &row, Some(Field::Null));
        test_eq(&mut null, &mut float2, &row, Some(Field::Null));
        test_eq(&mut null, &mut dec2, &row, Some(Field::Null));

        test_ne(&mut null, &mut uint2, &row, Some(Field::Null));
        test_ne(&mut null, &mut int2, &row, Some(Field::Null));
        test_ne(&mut null, &mut float2, &row, Some(Field::Null));
        test_ne(&mut null, &mut dec2, &row, Some(Field::Null));

        // gt: UInt
        if u_num1 > u_num2 && u_num1 as i64 > i_num1 && u_num1 as f64 > f_num1 && Decimal::from(u_num1) > d_num1.0 {
            test_gt(&mut uint1, &mut uint2, &row, None);
            test_gt(&mut uint1, &mut int1, &row, None);
            test_gt(&mut uint1, &mut float1, &row, None);
            test_gt(&mut uint1, &mut dec1, &row, None);
            test_gt(&mut uint1, &mut null, &row, Some(Field::Null));

            test_gte(&mut uint1, &mut uint2, &row, None);
            test_gte(&mut uint1, &mut int1, &row, None);
            test_gte(&mut uint1, &mut float1, &row, None);
            test_gte(&mut uint1, &mut dec1, &row, None);
            test_gte(&mut uint1, &mut null, &row, Some(Field::Null));

            test_lt(&mut uint1, &mut uint2, &row, Some(Field::Boolean(false)));
            test_lt(&mut uint1, &mut int1, &row, Some(Field::Boolean(false)));
            test_lt(&mut uint1, &mut float1, &row, Some(Field::Boolean(false)));
            test_lt(&mut uint1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_lt(&mut uint1, &mut null, &row, Some(Field::Null));

            test_lte(&mut uint1, &mut uint2, &row, Some(Field::Boolean(false)));
            test_lte(&mut uint1, &mut int1, &row, Some(Field::Boolean(false)));
            test_lte(&mut uint1, &mut float1, &row, Some(Field::Boolean(false)));
            test_lte(&mut uint1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_lte(&mut uint1, &mut null, &row, Some(Field::Null));
        }

        // gt: Int
        if i_num1 > u_num1 as i64 && i_num1 > i_num2 && i_num1 as f64 > f_num1 && Decimal::from(i_num1) > d_num1.0 {
            test_gt(&mut int1, &mut uint1, &row, None);
            test_gt(&mut int1, &mut int2, &row, None);
            test_gt(&mut int1, &mut float1, &row, None);
            test_gt(&mut int1, &mut dec1, &row, None);
            test_gt(&mut int1, &mut null, &row, Some(Field::Null));

            test_gte(&mut int1, &mut uint1, &row, None);
            test_gte(&mut int1, &mut int2, &row, None);
            test_gte(&mut int1, &mut float1, &row, None);
            test_gte(&mut int1, &mut dec1, &row, None);
            test_gte(&mut int1, &mut null, &row, Some(Field::Null));

            test_lt(&mut int1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_lt(&mut int1, &mut int2, &row, Some(Field::Boolean(false)));
            test_lt(&mut int1, &mut float1, &row, Some(Field::Boolean(false)));
            test_lt(&mut int1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_lt(&mut int1, &mut null, &row, Some(Field::Null));

            test_lte(&mut int1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_lte(&mut int1, &mut int2, &row, Some(Field::Boolean(false)));
            test_lte(&mut int1, &mut float1, &row, Some(Field::Boolean(false)));
            test_lte(&mut int1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_lte(&mut int1, &mut null, &row, Some(Field::Null));
        }

        // gt: Float
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 > u_num1 as f64 && f_num1 > i_num1 as f64 && f_num1 > f_num2 && d_val.unwrap() > d_num1.0 {
            test_gt(&mut float1, &mut uint1, &row, None);
            test_gt(&mut float1, &mut int1, &row, None);
            test_gt(&mut float1, &mut float2, &row, None);
            test_gt(&mut float1, &mut dec1, &row, None);
            test_gt(&mut float1, &mut null, &row, Some(Field::Null));

            test_gte(&mut float1, &mut uint1, &row, None);
            test_gte(&mut float1, &mut int1, &row, None);
            test_gte(&mut float1, &mut float2, &row, None);
            test_gte(&mut float1, &mut dec1, &row, None);
            test_gte(&mut float1, &mut null, &row, Some(Field::Null));

            test_lt(&mut float1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_lt(&mut float1, &mut int1, &row, Some(Field::Boolean(false)));
            test_lt(&mut float1, &mut float2, &row, Some(Field::Boolean(false)));
            test_lt(&mut float1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_lt(&mut float1, &mut null, &row, Some(Field::Null));

            test_lte(&mut float1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_lte(&mut float1, &mut int1, &row, Some(Field::Boolean(false)));
            test_lte(&mut float1, &mut float2, &row, Some(Field::Boolean(false)));
            test_lte(&mut float1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_lte(&mut float1, &mut null, &row, Some(Field::Null));
        }

        // gt: Decimal
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 > Decimal::from(u_num1) && d_num1.0 > Decimal::from(i_num1) && d_num1.0 > d_val.unwrap() && d_num1.0 > d_num2.0 {
            test_gt(&mut dec1, &mut uint1, &row, None);
            test_gt(&mut dec1, &mut int1, &row, None);
            test_gt(&mut dec1, &mut float1, &row, None);
            test_gt(&mut dec1, &mut dec2, &row, None);
            test_gt(&mut dec1, &mut null, &row, Some(Field::Null));

            test_gte(&mut dec1, &mut uint1, &row, None);
            test_gte(&mut dec1, &mut int1, &row, None);
            test_gte(&mut dec1, &mut float1, &row, None);
            test_gte(&mut dec1, &mut dec2, &row, None);
            test_gte(&mut dec1, &mut null, &row, Some(Field::Null));

            test_lt(&mut dec1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_lt(&mut dec1, &mut int1, &row, Some(Field::Boolean(false)));
            test_lt(&mut dec1, &mut float1, &row, Some(Field::Boolean(false)));
            test_lt(&mut dec1, &mut dec2, &row, Some(Field::Boolean(false)));
            test_lt(&mut dec1, &mut null, &row, Some(Field::Null));

            test_lte(&mut dec1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_lte(&mut dec1, &mut int1, &row, Some(Field::Boolean(false)));
            test_lt(&mut dec1, &mut float1, &row, Some(Field::Boolean(false)));
            test_lte(&mut dec1, &mut dec2, &row, Some(Field::Boolean(false)));
            test_lte(&mut dec1, &mut null, &row, Some(Field::Null));
        }

        // lt: UInt
        if u_num1 < u_num2 && (u_num1 as i64) < i_num1 && (u_num1 as f64) < f_num1 && Decimal::from(u_num1) < d_num1.0 {
            test_lt(&mut uint1, &mut uint2, &row, None);
            test_lt(&mut uint1, &mut int1, &row, None);
            test_lt(&mut uint1, &mut float1, &row, None);
            test_lt(&mut uint1, &mut dec1, &row, None);
            test_lt(&mut uint1, &mut null, &row, Some(Field::Null));

            test_lte(&mut uint1, &mut uint2, &row, None);
            test_lte(&mut uint1, &mut int1, &row, None);
            test_lte(&mut uint1, &mut float1, &row, None);
            test_lte(&mut uint1, &mut dec1, &row, None);
            test_lte(&mut uint1, &mut null, &row, Some(Field::Null));

            test_gt(&mut uint1, &mut uint2, &row, Some(Field::Boolean(false)));
            test_gt(&mut uint1, &mut int1, &row, Some(Field::Boolean(false)));
            test_gt(&mut uint1, &mut float1, &row, Some(Field::Boolean(false)));
            test_gt(&mut uint1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_gt(&mut uint1, &mut null, &row, Some(Field::Null));

            test_gte(&mut uint1, &mut uint2, &row, Some(Field::Boolean(false)));
            test_gte(&mut uint1, &mut int1, &row, Some(Field::Boolean(false)));
            test_gte(&mut uint1, &mut float1, &row, Some(Field::Boolean(false)));
            test_gte(&mut uint1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_gte(&mut uint1, &mut null, &row, Some(Field::Null));
        }

        // gt: Int
        if i_num1 < (u_num1 as i64) && i_num1 < i_num2 && (i_num1 as f64) < f_num1 && Decimal::from(i_num1) < d_num1.0 {
            test_lt(&mut int1, &mut uint1, &row, None);
            test_lt(&mut int1, &mut int2, &row, None);
            test_lt(&mut int1, &mut float1, &row, None);
            test_lt(&mut int1, &mut dec1, &row, None);
            test_lt(&mut int1, &mut null, &row, Some(Field::Null));

            test_lte(&mut int1, &mut uint1, &row, None);
            test_lte(&mut int1, &mut int2, &row, None);
            test_lte(&mut int1, &mut float1, &row, None);
            test_lte(&mut int1, &mut dec1, &row, None);
            test_lte(&mut int1, &mut null, &row, Some(Field::Null));

            test_gt(&mut int1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_gt(&mut int1, &mut int2, &row, Some(Field::Boolean(false)));
            test_gt(&mut int1, &mut float1, &row, Some(Field::Boolean(false)));
            test_gt(&mut int1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_gt(&mut int1, &mut null, &row, Some(Field::Null));

            test_gte(&mut int1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_gte(&mut int1, &mut int2, &row, Some(Field::Boolean(false)));
            test_gte(&mut int1, &mut float1, &row, Some(Field::Boolean(false)));
            test_gte(&mut int1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_gte(&mut int1, &mut null, &row, Some(Field::Null));
        }

        // gt: Float
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && f_num1 < u_num1 as f64 && f_num1 < i_num1 as f64 && f_num1 < f_num2 && d_val.unwrap() < d_num1.0 {
            test_lt(&mut float1, &mut uint1, &row, None);
            test_lt(&mut float1, &mut int1, &row, None);
            test_lt(&mut float1, &mut float2, &row, None);
            test_lt(&mut float1, &mut dec1, &row, None);
            test_lt(&mut float1, &mut null, &row, Some(Field::Null));

            test_lte(&mut float1, &mut uint1, &row, None);
            test_lte(&mut float1, &mut int1, &row, None);
            test_lte(&mut float1, &mut float2, &row, None);
            test_lte(&mut float1, &mut dec1, &row, None);
            test_lte(&mut float1, &mut null, &row, Some(Field::Null));

            test_gt(&mut float1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_gt(&mut float1, &mut int1, &row, Some(Field::Boolean(false)));
            test_gt(&mut float1, &mut float2, &row, Some(Field::Boolean(false)));
            test_gt(&mut float1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_gt(&mut float1, &mut null, &row, Some(Field::Null));

            test_gte(&mut float1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_gte(&mut float1, &mut int1, &row, Some(Field::Boolean(false)));
            test_gte(&mut float1, &mut float2, &row, Some(Field::Boolean(false)));
            test_gte(&mut float1, &mut dec1, &row, Some(Field::Boolean(false)));
            test_gte(&mut float1, &mut null, &row, Some(Field::Null));
        }

        // gt: Decimal
        let d_val = Decimal::from_f64(f_num1);
        if d_val.is_some() && d_num1.0 < Decimal::from(u_num1) && d_num1.0 < Decimal::from(i_num1) && d_num1.0 < d_val.unwrap() && d_num1.0 < d_num2.0 {
            test_lt(&mut dec1, &mut uint1, &row, None);
            test_lt(&mut dec1, &mut int1, &row, None);
            test_lt(&mut dec1, &mut float1, &row, None);
            test_lt(&mut dec1, &mut dec2, &row, None);
            test_lt(&mut dec1, &mut null, &row, Some(Field::Null));

            test_lte(&mut dec1, &mut uint1, &row, None);
            test_lte(&mut dec1, &mut int1, &row, None);
            test_lte(&mut dec1, &mut float1, &row, None);
            test_lte(&mut dec1, &mut dec2, &row, None);
            test_lte(&mut dec1, &mut null, &row, Some(Field::Null));

            test_gt(&mut dec1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_gt(&mut dec1, &mut int1, &row, Some(Field::Boolean(false)));
            test_gt(&mut dec1, &mut float1, &row, Some(Field::Boolean(false)));
            test_gt(&mut dec1, &mut dec2, &row, Some(Field::Boolean(false)));
            test_gt(&mut dec1, &mut null, &row, Some(Field::Null));

            test_gte(&mut dec1, &mut uint1, &row, Some(Field::Boolean(false)));
            test_gte(&mut dec1, &mut int1, &row, Some(Field::Boolean(false)));
            test_gte(&mut dec1, &mut float1, &row, Some(Field::Boolean(false)));
            test_gte(&mut dec1, &mut dec2, &row, Some(Field::Boolean(false)));
            test_gte(&mut dec1, &mut null, &row, Some(Field::Null));
        }
    });
}

fn test_eq(exp1: &mut Expression, exp2: &mut Expression, row: &Record, result: Option<Field>) {
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

fn test_ne(exp1: &mut Expression, exp2: &mut Expression, row: &Record, result: Option<Field>) {
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

fn test_gt(exp1: &mut Expression, exp2: &mut Expression, row: &Record, result: Option<Field>) {
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

fn test_lt(exp1: &mut Expression, exp2: &mut Expression, row: &Record, result: Option<Field>) {
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

fn test_gte(exp1: &mut Expression, exp2: &mut Expression, row: &Record, result: Option<Field>) {
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

fn test_lte(exp1: &mut Expression, exp2: &mut Expression, row: &Record, result: Option<Field>) {
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
