use crate::tests::{ArbitraryDateTime, ArbitraryDecimal};

use super::*;

use dozer_types::chrono::DateTime;
use dozer_types::types::{FieldDefinition, FieldType, Record, SourceDefinition};
use dozer_types::{
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    types::{Field, Schema},
};
use proptest::prelude::*;
use std::num::Wrapping;
use Expression::Literal;

#[test]
fn test_uint_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, u128_num1: u128, u128_num2: u128, i_num1: i64, i_num2: i64, i128_num1: i128, i128_num2: i128, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let mut uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let mut u128_1 = Box::new(Literal(Field::U128(u128_num1)));
        let mut u128_2 = Box::new(Literal(Field::U128(u128_num2)));
        let mut int1 = Box::new(Literal(Field::Int(i_num1)));
        let mut int2 = Box::new(Literal(Field::Int(i_num2)));
        let mut i128_1 = Box::new(Literal(Field::I128(i128_num1)));
        let mut i128_2 = Box::new(Literal(Field::I128(i128_num2)));
        let mut float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let mut float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let mut dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let mut dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let mut null = Box::new(Literal(Field::Null));

        //// left: UInt, right: UInt
        assert_eq!(
            // UInt + UInt = UInt
            evaluate_add(&Schema::default(), &mut uint1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num1) + Wrapping(u_num2)).0)
        );
        assert_eq!(
            // UInt - UInt = UInt
            evaluate_sub(&Schema::default(), &mut uint1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num1) - Wrapping(u_num2)).0)
        );
        assert_eq!(
            // UInt * UInt = UInt
            evaluate_mul(&Schema::default(), &mut uint2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num2) * Wrapping(u_num1)).0)
        );
        assert_eq!(
            // UInt / UInt = Float
            evaluate_div(&Schema::default(), &mut uint2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // UInt % UInt = UInt
            evaluate_mod(&Schema::default(), &mut uint1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num1) % Wrapping(u_num2)).0)
        );

        //// left: UInt, right: U128
        assert_eq!(
            // UInt + U128 = U128
            evaluate_add(&Schema::default(), &mut uint1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u_num1 as u128) + Wrapping(u128_num2)).0)
        );
        assert_eq!(
            // UInt - U128 = U128
            evaluate_sub(&Schema::default(), &mut uint1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u_num1 as u128) - Wrapping(u128_num2)).0)
        );
        assert_eq!(
            // UInt * U128 = U128
            evaluate_mul(&Schema::default(), &mut uint2, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u_num2 as u128) * Wrapping(u128_num1)).0)
        );
        assert_eq!(
            // UInt / U128 = Float
            evaluate_div(&Schema::default(), &mut uint2, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_u128(u128_num1).unwrap()))
        );
        assert_eq!(
            // UInt % U128 = U128
            evaluate_mod(&Schema::default(), &mut uint1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u_num1 as u128) % Wrapping(u128_num2)).0)
        );

        //// left: UInt, right: Int
        assert_eq!(
            // UInt + Int = Int
            evaluate_add(&Schema::default(), &mut uint1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num1 as i64) + Wrapping(i_num2)).0)
        );
        assert_eq!(
            // UInt - Int = Int
            evaluate_sub(&Schema::default(), &mut uint1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num1 as i64) - Wrapping(i_num2)).0)
        );
        assert_eq!(
            // UInt * Int = Int
            evaluate_mul(&Schema::default(), &mut uint2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num2 as i64) * Wrapping(i_num1)).0)
        );
        assert_eq!(
            // UInt / Int = Float
            evaluate_div(&Schema::default(), &mut uint2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // UInt % Int = Int
            evaluate_mod(&Schema::default(), &mut uint1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num1 as i64) % Wrapping(i_num2)).0)
        );

        //// left: UInt, right: I128
        assert_eq!(
            // UInt + I128 = I128
            evaluate_add(&Schema::default(), &mut uint1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u_num1 as i128) + Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // UInt - I128 = I128
            evaluate_sub(&Schema::default(), &mut uint1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u_num1 as i128) - Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // UInt * I128 = I128
            evaluate_mul(&Schema::default(), &mut uint2, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u_num2 as i128) * Wrapping(i128_num1)).0)
        );
        assert_eq!(
            // UInt / I128 = Float
            evaluate_div(&Schema::default(), &mut uint2, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_i128(i128_num1).unwrap()))
        );
        assert_eq!(
            // UInt % I128 = I128
            evaluate_mod(&Schema::default(), &mut uint1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u_num1 as i128) % Wrapping(i128_num2)).0)
        );

        //// left: UInt, right: Float
        assert_eq!(
            // UInt + Float = Float
            evaluate_add(&Schema::default(), &mut uint1, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() + f_num2))
        );
        assert_eq!(
            // UInt - Float = Float
            evaluate_sub(&Schema::default(), &mut uint1, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() - f_num2))
        );
        assert_eq!(
            // UInt * Float = Float
            evaluate_mul(&Schema::default(), &mut uint2, &mut float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() * f_num1))
        );
        if *float1 != Literal(Field::Float(OrderedFloat(0_f64))) {
            assert_eq!(
                // UInt / Float = Float
                evaluate_div(&Schema::default(), &mut uint2, &mut float1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f_num1))
            );
        }
        if *float2 != Literal(Field::Float(OrderedFloat(0_f64))) {
            assert_eq!(
                // UInt % Float = Float
                evaluate_mod(&Schema::default(), &mut uint1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() % f_num2))
            );
        }

        //// left: UInt, right: Decimal
        assert_eq!(
            // UInt + Decimal = Decimal
            evaluate_add(&Schema::default(), &mut uint1, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() + d_num2.0)
        );
        assert_eq!(
            // UInt - Decimal = Decimal
            evaluate_sub(&Schema::default(), &mut uint1, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() - d_num2.0)
        );
        // UInt * Decimal = Decimal
        let res = evaluate_mul(&Schema::default(), &mut uint2, &mut dec1, &row);
        if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_u64(u_num2).unwrap().checked_mul(d_num1.0).unwrap())
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
            ));
        }
        // UInt / Decimal = Decimal
        let res = evaluate_div(&Schema::default(), &mut uint2, &mut dec1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_u64(u_num2).unwrap() / d_num1.0)
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        // UInt % Decimal = Decimal
        let res = evaluate_mod(&Schema::default(), &mut uint2, &mut dec1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_u64(u_num2).unwrap() % d_num1.0)
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }

        //// left: UInt, right: Null
        assert_eq!(
            // UInt + Null = Null
            evaluate_add(&Schema::default(), &mut uint1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt - Null = Null
            evaluate_sub(&Schema::default(), &mut uint1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt * Null = Null
            evaluate_mul(&Schema::default(), &mut uint2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt / Null = Null
            evaluate_div(&Schema::default(), &mut uint2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt % Null = Null
            evaluate_mod(&Schema::default(), &mut uint1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_u128_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, u128_num1: u128, u128_num2: u128, i_num1: i64, i_num2: i64, i128_num1: i128, i128_num2: i128, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let mut uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let mut u128_1 = Box::new(Literal(Field::U128(u128_num1)));
        let mut u128_2 = Box::new(Literal(Field::U128(u128_num2)));
        let mut int1 = Box::new(Literal(Field::Int(i_num1)));
        let mut int2 = Box::new(Literal(Field::Int(i_num2)));
        let mut i128_1 = Box::new(Literal(Field::I128(i128_num1)));
        let mut i128_2 = Box::new(Literal(Field::I128(i128_num2)));
        let mut float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let mut float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let mut dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let mut dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let mut null = Box::new(Literal(Field::Null));

        //// left: U128, right: UInt
        assert_eq!(
            // U128 + UInt = U128
            evaluate_add(&Schema::default(), &mut u128_1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num1) + Wrapping(u_num2 as u128)).0)
        );
        assert_eq!(
            // U128 - UInt = U128
            evaluate_sub(&Schema::default(), &mut u128_1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num1) - Wrapping(u_num2 as u128)).0)
        );
        assert_eq!(
            // U128 * UInt = U128
            evaluate_mul(&Schema::default(), &mut u128_2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num2) * Wrapping(u_num1 as u128)).0)
        );
        assert_eq!(
            // U128 / UInt = Float
            evaluate_div(&Schema::default(), &mut u128_2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u128(u128_num2).unwrap() / f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // U128 % UInt = U128
            evaluate_mod(&Schema::default(), &mut u128_1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num1) % Wrapping(u_num2 as u128)).0)
        );

        //// left: U128, right: U128
        assert_eq!(
            // U128 + U128 = U128
            evaluate_add(&Schema::default(), &mut u128_1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num1) + Wrapping(u128_num2)).0)
        );
        assert_eq!(
            // U128 - U128 = U128
            evaluate_sub(&Schema::default(), &mut u128_1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num1) - Wrapping(u128_num2)).0)
        );
        assert_eq!(
            // U128 * U128 = U128
            evaluate_mul(&Schema::default(), &mut u128_2, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num2) * Wrapping(u128_num1)).0)
        );
        assert_eq!(
            // U128 / U128 = Float
            evaluate_div(&Schema::default(), &mut u128_2, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u128(u128_num2).unwrap() / f64::from_u128(u128_num1).unwrap()))
        );
        assert_eq!(
            // U128 % U128 = U128
            evaluate_mod(&Schema::default(), &mut u128_1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::U128((Wrapping(u128_num1) % Wrapping(u128_num2)).0)
        );

        //// left: U128, right: Int
        assert_eq!(
            // U128 + Int = I128
            evaluate_add(&Schema::default(), &mut u128_1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num1 as i128) + Wrapping(i_num2 as i128)).0)
        );
        assert_eq!(
            // U128 - Int = I128
            evaluate_sub(&Schema::default(), &mut u128_1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num1 as i128) - Wrapping(i_num2 as i128)).0)
        );
        assert_eq!(
            // U128 * Int = I128
            evaluate_mul(&Schema::default(), &mut u128_2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num2 as i128) * Wrapping(i_num1 as i128)).0)
        );
        assert_eq!(
            // U128 / Int = Float
            evaluate_div(&Schema::default(), &mut u128_2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u128(u128_num2).unwrap() / f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // U128 % Int = I128
            evaluate_mod(&Schema::default(), &mut u128_1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num1 as i128) % Wrapping(i_num2 as i128)).0)
        );

        //// left: U128, right: I128
        assert_eq!(
            // U128 + I128 = I128
            evaluate_add(&Schema::default(), &mut u128_1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num1 as i128) + Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // U128 - I128 = I128
            evaluate_sub(&Schema::default(), &mut u128_1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num1 as i128) - Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // U128 * I128 = I128
            evaluate_mul(&Schema::default(), &mut u128_2, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num2 as i128) * Wrapping(i128_num1)).0)
        );
        assert_eq!(
            // U128 / I128 = Float
            evaluate_div(&Schema::default(), &mut u128_2, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u128(u128_num2).unwrap() / f64::from_i128(i128_num1).unwrap()))
        );
        assert_eq!(
            // U128 % I128 = I128
            evaluate_mod(&Schema::default(), &mut u128_1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(u128_num1 as i128) % Wrapping(i128_num2)).0)
        );

        //// left: U128, right: Float
        let res = evaluate_add(&Schema::default(), &mut u128_1, &mut float2, &row);
        if res.is_ok() {
            assert_eq!(
                // U128 + Float = Float
                evaluate_add(&Schema::default(), &mut u128_1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_u128(u128_num1).unwrap() + f_num2))
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut u128_1, &mut float2, &row);
        if res.is_ok() {
            assert_eq!(
                // U128 - Float = Float
                evaluate_sub(&Schema::default(), &mut u128_1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_u128(u128_num1).unwrap() - f_num2))
            );
        }
        let res = evaluate_mul(&Schema::default(), &mut u128_2, &mut float1, &row);
        if res.is_ok() {
            assert_eq!(
                // U128 * Float = Float
                evaluate_mul(&Schema::default(), &mut u128_2, &mut float1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_u128(u128_num2).unwrap() * f_num1))
            );
        }
        let res = evaluate_div(&Schema::default(), &mut u128_2, &mut float1, &row);
        if res.is_ok() {
            assert_eq!(
                // U128 / Float = Float
                evaluate_div(&Schema::default(), &mut u128_2, &mut float1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_u128(u128_num2).unwrap() / f_num1))
            );
        }
        let res = evaluate_mod(&Schema::default(), &mut u128_1, &mut float2, &row);
        if res.is_ok() {
            assert_eq!(
                // U128 % Float = Float
                evaluate_mod(&Schema::default(), &mut u128_1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_u128(u128_num1).unwrap() % f_num2))
            );
        }

        //// left: U128, right: Decimal
        let res = evaluate_add(&Schema::default(), &mut u128_1, &mut dec2, &row);
        if res.is_ok() {
            assert_eq!(
                // U128 + Decimal = Decimal
                evaluate_add(&Schema::default(), &mut u128_1, &mut dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(Decimal::from_u128(u128_num1).unwrap() + d_num2.0)
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut u128_1, &mut dec2, &row);
        if res.is_ok() {
            assert_eq!(
                // U128 - Decimal = Decimal
                evaluate_sub(&Schema::default(), &mut u128_1, &mut dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(Decimal::from_u128(u128_num1).unwrap() - d_num2.0)
            );
        }
        // U128 * Decimal = Decimal
        let res = evaluate_mul(&Schema::default(), &mut u128_2, &mut dec1, &row);
        if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_u128(u128_num2).unwrap().checked_mul(d_num1.0).unwrap())
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
                ));
            }
        }
        // U128 / Decimal = Decimal
        let res = evaluate_div(&Schema::default(), &mut u128_2, &mut dec1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_u128(u128_num2).unwrap() / d_num1.0)
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
        }
        // U128 % Decimal = Decimal
        let res = evaluate_mod(&Schema::default(), &mut u128_1, &mut dec1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_u128(u128_num1).unwrap() % d_num1.0)
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }

        //// left: U128, right: Null
        assert_eq!(
            // U128 + Null = Null
            evaluate_add(&Schema::default(), &mut u128_1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // U128 - Null = Null
            evaluate_sub(&Schema::default(), &mut u128_1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // U128 * Null = Null
            evaluate_mul(&Schema::default(), &mut u128_2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // U128 / Null = Null
            evaluate_div(&Schema::default(), &mut u128_2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // U128 % Null = Null
            evaluate_mod(&Schema::default(), &mut u128_1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_int_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, u128_num1: u128, u128_num2: u128, i_num1: i64, i_num2: i64, i128_num1: i128, i128_num2: i128, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let mut uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let mut u128_1 = Box::new(Literal(Field::U128(u128_num1)));
        let mut u128_2 = Box::new(Literal(Field::U128(u128_num2)));
        let mut int1 = Box::new(Literal(Field::Int(i_num1)));
        let mut int2 = Box::new(Literal(Field::Int(i_num2)));
        let mut i128_1 = Box::new(Literal(Field::I128(i128_num1)));
        let mut i128_2 = Box::new(Literal(Field::I128(i128_num2)));
        let mut float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let mut float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let mut dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let mut dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let mut null = Box::new(Literal(Field::Null));

        //// left: Int, right: UInt
        assert_eq!(
            // Int + UInt = Int
            evaluate_add(&Schema::default(), &mut int1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) + Wrapping(u_num2 as i64)).0)
        );
        assert_eq!(
            // Int - UInt = Int
            evaluate_sub(&Schema::default(), &mut int1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) - Wrapping(u_num2 as i64)).0)
        );
        assert_eq!(
            // Int * UInt = Int
            evaluate_mul(&Schema::default(), &mut int2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num2) * Wrapping(u_num1 as i64)).0)
        );
        assert_eq!(
            // Int / UInt = Float
            evaluate_div(&Schema::default(), &mut int2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() / f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // Int % UInt = Int
            evaluate_mod(&Schema::default(), &mut int1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) % Wrapping(u_num2 as i64)).0)
        );

        //// left: Int, right: U128
        assert_eq!(
            // Int + U128 = I128
            evaluate_add(&Schema::default(), &mut int1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num1 as i128) + Wrapping(u128_num2 as i128)).0)
        );
        assert_eq!(
            // Int - U128 = I128
            evaluate_sub(&Schema::default(), &mut int1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num1 as i128) - Wrapping(u128_num2 as i128)).0)
        );
        assert_eq!(
            // Int * U128 = I128
            evaluate_mul(&Schema::default(), &mut int2, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num2 as i128) * Wrapping(u128_num1 as i128)).0)
        );
        let res = evaluate_div(&Schema::default(), &mut int2, &mut u128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Int / U128 = Float
                evaluate_div(&Schema::default(), &mut int2, &mut u128_1, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i_num2 as i128).unwrap() / f64::from_i128(u128_num1 as i128).unwrap()))
            );
        }
        assert_eq!(
            // Int % U128 = I128
            evaluate_mod(&Schema::default(), &mut int1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num1 as i128) % Wrapping(u128_num2 as i128)).0)
        );

        //// left: Int, right: Int
        assert_eq!(
            // Int + Int = Int
            evaluate_add(&Schema::default(), &mut int1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) + Wrapping(i_num2)).0)
        );
        assert_eq!(
            // Int - Int = Int
            evaluate_sub(&Schema::default(), &mut int1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) - Wrapping(i_num2)).0)
        );
        assert_eq!(
            // Int * Int = Int
            evaluate_mul(&Schema::default(), &mut int2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num2) * Wrapping(i_num1)).0)
        );
        assert_eq!(
            // Int / Int = Float
            evaluate_div(&Schema::default(), &mut int2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() / f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // Int % Int = Int
            evaluate_mod(&Schema::default(), &mut int1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) % Wrapping(i_num2)).0)
        );

        //// left: Int, right: I128
        assert_eq!(
            // Int + I128 = I128
            evaluate_add(&Schema::default(), &mut int1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num1 as i128) + Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // Int - I128 = I128
            evaluate_sub(&Schema::default(), &mut int1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num1 as i128) - Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // Int * I128 = I128
            evaluate_mul(&Schema::default(), &mut int2, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num2 as i128) * Wrapping(i128_num1)).0)
        );
        let res = evaluate_div(&Schema::default(), &mut int2, &mut i128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Int / I128 = Float
                evaluate_div(&Schema::default(), &mut int2, &mut i128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() / f64::from_i128(i128_num1).unwrap()))
            );
        }
        assert_eq!(
            // Int % I128 = I128
            evaluate_mod(&Schema::default(), &mut int1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i_num1 as i128) % Wrapping(i128_num2)).0)
        );

        //// left: Int, right: Float
        assert_eq!(
            // Int + Float = Float
            evaluate_add(&Schema::default(), &mut int1, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num1).unwrap() + f_num2))
        );
        assert_eq!(
            // Int - Float = Float
            evaluate_sub(&Schema::default(), &mut int1, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num1).unwrap() - f_num2))
        );
        assert_eq!(
            // Int * Float = Float
            evaluate_mul(&Schema::default(), &mut int2, &mut float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() * f_num1))
        );
        if *float1 != Literal(Field::Float(OrderedFloat(0_f64))) {
            assert_eq!(
                // Int / Float = Float
                evaluate_div(&Schema::default(), &mut int2, &mut float1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() / f_num1))
            );
        }
        if *float2 != Literal(Field::Float(OrderedFloat(0_f64))) {
            assert_eq!(
                // Int % Float = Float
                evaluate_mod(&Schema::default(), &mut int1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i64(i_num1).unwrap() % f_num2))
            );
        }

        //// left: Int, right: Decimal
        assert_eq!(
            // Int + Decimal = Decimal
            evaluate_add(&Schema::default(), &mut int1, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_i64(i_num1).unwrap() + d_num2.0)
        );
        assert_eq!(
            // Int - Decimal = Decimal
            evaluate_sub(&Schema::default(), &mut int1, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_i64(i_num1).unwrap() - d_num2.0)
        );
        // Int * Decimal = Decimal
        let res = evaluate_mul(&Schema::default(), &mut int2, &mut dec1, &row);
        if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_i64(i_num2).unwrap().checked_mul(d_num1.0).unwrap())
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
            ));
        }
        // Int / Decimal = Decimal
        let res = evaluate_div(&Schema::default(), &mut int2, &mut dec1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_i64(i_num2).unwrap() / d_num1.0)
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        // Int % Decimal = Decimal
        let res = evaluate_mod(&Schema::default(), &mut int1, &mut dec2, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_i64(i_num1).unwrap() % d_num2.0)
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }

        //// left: Int, right: Null
        assert_eq!(
            // Int + Null = Null
            evaluate_add(&Schema::default(), &mut int1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int - Null = Null
            evaluate_sub(&Schema::default(), &mut int1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int * Null = Null
            evaluate_mul(&Schema::default(), &mut int2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int / Null = Null
            evaluate_div(&Schema::default(), &mut int2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int % Null = Null
            evaluate_mod(&Schema::default(), &mut int1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_i128_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, u128_num1: u128, u128_num2: u128, i_num1: i64, i_num2: i64, i128_num1: i128, i128_num2: i128, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let mut uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let mut u128_1 = Box::new(Literal(Field::U128(u128_num1)));
        let mut u128_2 = Box::new(Literal(Field::U128(u128_num2)));
        let mut int1 = Box::new(Literal(Field::Int(i_num1)));
        let mut int2 = Box::new(Literal(Field::Int(i_num2)));
        let mut i128_1 = Box::new(Literal(Field::I128(i128_num1)));
        let mut i128_2 = Box::new(Literal(Field::I128(i128_num2)));
        let mut float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let mut float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let mut dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let mut dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let mut null = Box::new(Literal(Field::Null));

        //// left: I128, right: UInt
        assert_eq!(
            // I128 + UInt = I128
            evaluate_add(&Schema::default(), &mut i128_1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) + Wrapping(u_num2 as i128)).0)
        );
        assert_eq!(
            // I128 - UInt = I128
            evaluate_sub(&Schema::default(), &mut i128_1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) - Wrapping(u_num2 as i128)).0)
        );
        assert_eq!(
            // I128 * UInt = I128
            evaluate_mul(&Schema::default(), &mut i128_2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num2) * Wrapping(u_num1 as i128)).0)
        );
        let res = evaluate_div(&Schema::default(), &mut i128_2, &mut uint1, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 / UInt = Float
                evaluate_div(&Schema::default(), &mut i128_2, &mut uint1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num2).unwrap() / f64::from_u64(u_num1).unwrap()))
            );
        }
        assert_eq!(
            // I128 % UInt = I128
            evaluate_mod(&Schema::default(), &mut i128_1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) % Wrapping(u_num2 as i128)).0)
        );

        //// left: I128, right: U128
        assert_eq!(
            // I128 + U128 = I128
            evaluate_add(&Schema::default(), &mut i128_1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) + Wrapping(u128_num2 as i128)).0)
        );
        assert_eq!(
            // I128 - U128 = I128
            evaluate_sub(&Schema::default(), &mut i128_1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) - Wrapping(u128_num2 as i128)).0)
        );
        assert_eq!(
            // I128 * U128 = I128
            evaluate_mul(&Schema::default(), &mut i128_2, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num2) * Wrapping(u128_num1 as i128)).0)
        );
        let res = evaluate_div(&Schema::default(), &mut i128_2, &mut u128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 / U128 = Float
                evaluate_div(&Schema::default(), &mut i128_2, &mut u128_1, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num2).unwrap() / f64::from_i128(u128_num1 as i128).unwrap()))
            );
        }
        assert_eq!(
            // I128 % U128 = I128
            evaluate_mod(&Schema::default(), &mut i128_1, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) % Wrapping(u128_num2 as i128)).0)
        );

        //// left: I128, right: Int
        assert_eq!(
            // I128 + Int = I128
            evaluate_add(&Schema::default(), &mut i128_1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) + Wrapping(i_num2 as i128)).0)
        );
        assert_eq!(
            // I128 - Int = I128
            evaluate_sub(&Schema::default(), &mut i128_1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) - Wrapping(i_num2 as i128)).0)
        );
        assert_eq!(
            // I128 * Int = I128
            evaluate_mul(&Schema::default(), &mut i128_2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num2) * Wrapping(i_num1 as i128)).0)
        );
        let res = evaluate_div(&Schema::default(), &mut i128_2, &mut int1, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 / Int = Float
                evaluate_div(&Schema::default(), &mut i128_2, &mut int1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num2).unwrap() / f64::from_i64(i_num1).unwrap()))
            );
        }
        assert_eq!(
            // I128 % Int = I128
            evaluate_mod(&Schema::default(), &mut i128_1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) % Wrapping(i_num2 as i128)).0)
        );

        //// left: I128, right: I128
        assert_eq!(
            // I128 + I128 = I128
            evaluate_add(&Schema::default(), &mut i128_1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) + Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // I128 - I128 = I128
            evaluate_sub(&Schema::default(), &mut i128_1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) - Wrapping(i128_num2)).0)
        );
        assert_eq!(
            // I128 * I128 = I128
            evaluate_mul(&Schema::default(), &mut i128_2, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num2) * Wrapping(i128_num1)).0)
        );
        let res = evaluate_div(&Schema::default(), &mut i128_2, &mut i128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 / I128 = Float
                evaluate_div(&Schema::default(), &mut i128_2, &mut i128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num2).unwrap() / f64::from_i128(i128_num1).unwrap()))
            );
        }
        assert_eq!(
            // I128 % I128 = I128
            evaluate_mod(&Schema::default(), &mut i128_1, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::I128((Wrapping(i128_num1) % Wrapping(i128_num2)).0)
        );

        //// left: I128, right: Float
        let res = evaluate_add(&Schema::default(), &mut i128_1, &mut float2, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 + Float = Float
                evaluate_add(&Schema::default(), &mut i128_1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num1).unwrap() + f_num2))
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut i128_1, &mut float2, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 - Float = Float
                evaluate_sub(&Schema::default(), &mut i128_1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num1).unwrap() - f_num2))
            );
        }
        let res = evaluate_mul(&Schema::default(), &mut i128_2, &mut float1, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 * Float = Float
                evaluate_mul(&Schema::default(), &mut i128_2, &mut float1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num2).unwrap() * f_num1))
            );
        }
        let res = evaluate_div(&Schema::default(), &mut i128_2, &mut float1, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 / Float = Float
                evaluate_div(&Schema::default(), &mut i128_2, &mut float1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num2).unwrap() / f_num1))
            );
        }
        let res = evaluate_mod(&Schema::default(), &mut i128_1, &mut float2, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 % Float = Float
                evaluate_mod(&Schema::default(), &mut i128_1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f64::from_i128(i128_num1).unwrap() % f_num2))
            );
        }

        //// left: I128, right: Decimal
        let res = evaluate_add(&Schema::default(), &mut i128_1, &mut dec2, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 + Decimal = Decimal
                evaluate_add(&Schema::default(), &mut i128_1, &mut dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(Decimal::from_i128(i128_num1).unwrap() + d_num2.0)
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut i128_1, &mut dec2, &row);
        if res.is_ok() {
            assert_eq!(
                // I128 - Decimal = Decimal
                evaluate_sub(&Schema::default(), &mut i128_1, &mut dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(Decimal::from_i128(i128_num1).unwrap() - d_num2.0)
            );
        }
        // I128 * Decimal = Decimal
        let res = evaluate_mul(&Schema::default(), &mut i128_2, &mut dec1, &row);
        if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_i128(i128_num2).unwrap().checked_mul(d_num1.0).unwrap())
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
                ));
            }
        }
        // I128 / Decimal = Decimal
        let res = evaluate_div(&Schema::default(), &mut i128_2, &mut dec1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_i128(i128_num2).unwrap() / d_num1.0)
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
        }
        // I128 % Decimal = Decimal
        let res = evaluate_mod(&Schema::default(), &mut i128_1, &mut dec2, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(Decimal::from_i128(i128_num1).unwrap() % d_num2.0)
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }

        //// left: I128, right: Null
        assert_eq!(
            // I128 + Null = Null
            evaluate_add(&Schema::default(), &mut i128_1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // I128 - Null = Null
            evaluate_sub(&Schema::default(), &mut i128_1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // I128 * Null = Null
            evaluate_mul(&Schema::default(), &mut i128_2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // I128 / Null = Null
            evaluate_div(&Schema::default(), &mut i128_2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // I128 % Null = Null
            evaluate_mod(&Schema::default(), &mut i128_1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_float_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, u128_num1: u128, u128_num2: u128, i_num1: i64, i_num2: i64, i128_num1: i128, i128_num2: i128, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let mut uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let mut u128_1 = Box::new(Literal(Field::U128(u128_num1)));
        let mut u128_2 = Box::new(Literal(Field::U128(u128_num2)));
        let mut int1 = Box::new(Literal(Field::Int(i_num1)));
        let mut int2 = Box::new(Literal(Field::Int(i_num2)));
        let mut i128_1 = Box::new(Literal(Field::I128(i128_num1)));
        let mut i128_2 = Box::new(Literal(Field::I128(i128_num2)));
        let mut float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let mut float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let mut dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let mut dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let mut null = Box::new(Literal(Field::Null));

        //// left: Float, right: UInt
        assert_eq!(
            // Float + UInt = Float
            evaluate_add(&Schema::default(), &mut float1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) + OrderedFloat(f64::from_u64(u_num2).unwrap()))
        );
        assert_eq!(
            // Float - UInt = Float
            evaluate_sub(&Schema::default(), &mut float1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) - OrderedFloat(f64::from_u64(u_num2).unwrap()))
        );
        assert_eq!(
            // Float * UInt = Float
            evaluate_mul(&Schema::default(), &mut float2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) * OrderedFloat(f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // Float / UInt = Float
            evaluate_div(&Schema::default(), &mut float2, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) / OrderedFloat(f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // Float % UInt = Float
            evaluate_mod(&Schema::default(), &mut float1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) % OrderedFloat(f64::from_u64(u_num2).unwrap()))
        );

        //// left: Float, right: U128
        let res = evaluate_add(&Schema::default(), &mut float1, &mut u128_2, &row);
        if res.is_ok() {
           assert_eq!(
                // Float + U128 = Float
                evaluate_add(&Schema::default(), &mut float1, &mut u128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num1) + OrderedFloat(f64::from_u128(u128_num2).unwrap()))
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut float1, &mut u128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Float - U128 = Float
                evaluate_sub(&Schema::default(), &mut float1, &mut u128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num1) - OrderedFloat(f64::from_u128(u128_num2).unwrap()))
            );
        }
        let res = evaluate_mul(&Schema::default(), &mut float2, &mut u128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Float * U128 = Float
                evaluate_mul(&Schema::default(), &mut float2, &mut u128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num2) * OrderedFloat(f64::from_u128(u128_num1).unwrap()))
            );
        }
        let res = evaluate_div(&Schema::default(), &mut float2, &mut u128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Float / U128 = Float
                evaluate_div(&Schema::default(), &mut float2, &mut u128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num2) / OrderedFloat(f64::from_u128(u128_num1).unwrap()))
            );
        }
        let res = evaluate_mod(&Schema::default(), &mut float1, &mut u128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Float % U128 = Float
                evaluate_mod(&Schema::default(), &mut float1, &mut u128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num1) % OrderedFloat(f64::from_u128(u128_num2).unwrap()))
            );
        }

        //// left: Float, right: Int
        assert_eq!(
            // Float + Int = Float
            evaluate_add(&Schema::default(), &mut float1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) + OrderedFloat(f64::from_i64(i_num2).unwrap()))
        );
        assert_eq!(
            // Float - Int = Float
            evaluate_sub(&Schema::default(), &mut float1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) - OrderedFloat(f64::from_i64(i_num2).unwrap()))
        );
        assert_eq!(
            // Float * Int = Float
            evaluate_mul(&Schema::default(), &mut float2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) * OrderedFloat(f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // Float / Int = Float
            evaluate_div(&Schema::default(), &mut float2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) / OrderedFloat(f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // Float % Int = Float
            evaluate_mod(&Schema::default(), &mut float1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) % OrderedFloat(f64::from_i64(i_num2).unwrap()))
        );

        //// left: Float, right: I128
        let res = evaluate_add(&Schema::default(), &mut float1, &mut i128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Float + I128 = Float
                evaluate_add(&Schema::default(), &mut float1, &mut i128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num1) + OrderedFloat(f64::from_i128(i128_num2).unwrap()))
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut float1, &mut i128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Float - I128 = Float
                evaluate_sub(&Schema::default(), &mut float1, &mut i128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num1) - OrderedFloat(f64::from_i128(i128_num2).unwrap()))
            );
        }
        let res = evaluate_mul(&Schema::default(), &mut float2, &mut i128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Float * I128 = Float
                evaluate_mul(&Schema::default(), &mut float2, &mut i128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num2) * OrderedFloat(f64::from_i128(i128_num1).unwrap()))
            );
        }
        let res = evaluate_div(&Schema::default(), &mut float2, &mut i128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Float / I128 = Float
                evaluate_div(&Schema::default(), &mut float2, &mut i128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num2) / OrderedFloat(f64::from_i128(i128_num1).unwrap()))
            );
        }
        let res = evaluate_mod(&Schema::default(), &mut float1, &mut i128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Float % I128 = Float
                evaluate_mod(&Schema::default(), &mut float1, &mut i128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num1) % OrderedFloat(f64::from_i128(i128_num2).unwrap()))
            );
        }

        //// left: Float, right: Float
        assert_eq!(
            // Float + Float = Float
            evaluate_add(&Schema::default(), &mut float1, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1 + f_num2))
        );
        assert_eq!(
            // Float - Float = Float
            evaluate_sub(&Schema::default(), &mut float1, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1 - f_num2))
        );
        assert_eq!(
            // Float * Float = Float
            evaluate_mul(&Schema::default(), &mut float2, &mut float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2 * f_num1))
        );
        if *float1 != Literal(Field::Float(OrderedFloat(0_f64))) {
            assert_eq!(
                // Float / Float = Float
                evaluate_div(&Schema::default(), &mut float2, &mut float1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num2 / f_num1))
            );
        }
        if *float2 != Literal(Field::Float(OrderedFloat(0_f64))) {
            assert_eq!(
                // Float % Float = Float
                evaluate_mod(&Schema::default(), &mut float1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num1 % f_num2))
            );
        }

        //// left: Float, right: Decimal
        let d_val1 = Decimal::from_f64(f_num1);
        let d_val2 = Decimal::from_f64(f_num2);
        if d_val1.is_some() && d_val2.is_some() {
            assert_eq!(
                // Float + Decimal = Decimal
                evaluate_add(&Schema::default(), &mut float1, &mut dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_val1.unwrap() + d_num2.0)
            );
            assert_eq!(
                // Float - Decimal = Decimal
                evaluate_sub(&Schema::default(), &mut float1, &mut dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_val1.unwrap() - d_num2.0)
            );
            // Float * Decimal = Decimal
            let res = evaluate_mul(&Schema::default(), &mut float2, &mut dec1, &row);
            if res.is_ok() {
                 assert_eq!(
                    res.unwrap(), Field::Decimal(d_val2.unwrap().checked_mul(d_num1.0).unwrap())
                );
            } else {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
                ));
            }
            // Float / Decimal = Decimal
            let res = evaluate_div(&Schema::default(), &mut float2, &mut dec1, &row);
            if d_num1.0 == Decimal::new(0, 0) {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
            else if res.is_ok() {
                 assert_eq!(
                    res.unwrap(), Field::Decimal(d_val2.unwrap().checked_div(d_num1.0).unwrap())
                );
            } else {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
            // Float % Decimal = Decimal
            let res = evaluate_mod(&Schema::default(), &mut float1, &mut dec2, &row);
            if d_num1.0 == Decimal::new(0, 0) {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
            else if res.is_ok() {
                 assert_eq!(
                    res.unwrap(), Field::Decimal(d_val1.unwrap().checked_rem(d_num2.0).unwrap())
                );
            } else {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }

        //// left: Float, right: Null
        assert_eq!(
            // Float + Null = Null
            evaluate_add(&Schema::default(), &mut float1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float - Null = Null
            evaluate_sub(&Schema::default(), &mut float1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float * Null = Null
            evaluate_mul(&Schema::default(), &mut float2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float / Null = Null
            evaluate_div(&Schema::default(), &mut float2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float % Null = Null
            evaluate_mod(&Schema::default(), &mut float1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_decimal_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, u128_num1: u128, u128_num2: u128, i_num1: i64, i_num2: i64, i128_num1: i128, i128_num2: i128, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let mut uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let mut u128_1 = Box::new(Literal(Field::U128(u128_num1)));
        let mut u128_2 = Box::new(Literal(Field::U128(u128_num2)));
        let mut int1 = Box::new(Literal(Field::Int(i_num1)));
        let mut int2 = Box::new(Literal(Field::Int(i_num2)));
        let mut i128_1 = Box::new(Literal(Field::I128(i128_num1)));
        let mut i128_2 = Box::new(Literal(Field::I128(i128_num2)));
        let mut float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let mut float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let mut dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let mut dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let mut null = Box::new(Literal(Field::Null));

        //// left: Decimal, right: UInt
        assert_eq!(
            // Decimal + UInt = Decimal
            evaluate_add(&Schema::default(), &mut dec1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 + Decimal::from(u_num2))
        );
        assert_eq!(
            // Decimal - UInt = Decimal
            evaluate_sub(&Schema::default(), &mut dec1, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 - Decimal::from(u_num2))
        );
        // Decimal * UInt = Decimal
        let res = evaluate_mul(&Schema::default(), &mut dec2, &mut uint1, &row);
        if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num2.0 * Decimal::from(u_num1))
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
            ));
        }
        // Decimal / UInt = Decimal
        let res = evaluate_div(&Schema::default(), &mut dec2, &mut uint1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num2.0 / Decimal::from(u_num1))
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        // Decimal % UInt = Decimal
        let res = evaluate_mod(&Schema::default(), &mut dec1, &mut uint2, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num1.0 % Decimal::from(u_num2))
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }

        //// left: Decimal, right: U128
        let res = evaluate_add(&Schema::default(), &mut dec1, &mut u128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal + U128 = Decimal
                evaluate_add(&Schema::default(), &mut dec1, &mut u128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 + Decimal::from_u128(u128_num2).unwrap())
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut dec1, &mut u128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal - U128 = Decimal
                evaluate_sub(&Schema::default(), &mut dec1, &mut u128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 - Decimal::from_u128(u128_num2).unwrap())
            );
        }
        // Decimal * U128 = Decimal
        let res = evaluate_mul(&Schema::default(), &mut dec2, &mut u128_1, &row);
        if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num2.0 * Decimal::from_u128(u128_num1).unwrap())
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
                ));
            }
        }
        // Decimal / U128 = Decimal
        let res = evaluate_div(&Schema::default(), &mut dec2, &mut u128_1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num2.0 / Decimal::from_u128(u128_num1).unwrap())
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
        }
        // Decimal % U128 = Decimal
        let res = evaluate_mod(&Schema::default(), &mut dec1, &mut u128_2, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num1.0 % Decimal::from_u128(u128_num2).unwrap())
            );
        } else {
            assert!(res.is_err());
            if !matches!(res, Err(PipelineError::UnableToCast(_, _))) {
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }

        //// left: Decimal, right: Int
        assert_eq!(
            // Decimal + Int = Decimal
            evaluate_add(&Schema::default(), &mut dec1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 + Decimal::from(i_num2))
        );
        assert_eq!(
            // Decimal - Int = Decimal
            evaluate_sub(&Schema::default(), &mut dec1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 - Decimal::from(i_num2))
        );
        let res = evaluate_mul(&Schema::default(), &mut dec2, &mut int1, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal * Int = Decimal
                evaluate_mul(&Schema::default(), &mut dec2, &mut int1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num2.0 * Decimal::from(i_num1))
            );
        }
        assert_eq!(
            // Decimal / Int = Decimal
            evaluate_div(&Schema::default(), &mut dec2, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num2.0 / Decimal::from(i_num1))
        );
        assert_eq!(
            // Decimal % Int = Decimal
            evaluate_mod(&Schema::default(), &mut dec1, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 % Decimal::from(i_num2))
        );

        //// left: Decimal, right: I128
        let res = evaluate_add(&Schema::default(), &mut dec1, &mut i128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal + I128 = Decimal
                evaluate_add(&Schema::default(), &mut dec1, &mut i128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 + Decimal::from_i128(i128_num2).unwrap())
            );
        }
        let res = evaluate_sub(&Schema::default(), &mut dec1, &mut i128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal - I128 = Decimal
                evaluate_sub(&Schema::default(), &mut dec1, &mut i128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 - Decimal::from_i128(i128_num2).unwrap())
            );
        }
        let res = evaluate_mul(&Schema::default(), &mut dec2, &mut i128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal * I128 = Decimal
                evaluate_mul(&Schema::default(), &mut dec2, &mut i128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num2.0 * Decimal::from_i128(i128_num1).unwrap())
            );
        }
        let res = evaluate_div(&Schema::default(), &mut dec2, &mut i128_1, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal / I128 = Decimal
                evaluate_div(&Schema::default(), &mut dec2, &mut i128_1, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num2.0 / Decimal::from_i128(i128_num1).unwrap())
            );
        }
        let res = evaluate_mod(&Schema::default(), &mut dec1, &mut i128_2, &row);
        if res.is_ok() {
            assert_eq!(
                // Decimal % I128 = Decimal
                evaluate_mod(&Schema::default(), &mut dec1, &mut i128_2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 % Decimal::from_i128(i128_num2).unwrap())
            );
        }

        // left: Decimal, right: Float
        let d_val1 = Decimal::from_f64(f_num1);
        let d_val2 = Decimal::from_f64(f_num2);
        if d_val1.is_some() && d_val2.is_some() && d_val1.unwrap() != Decimal::new(0, 0) && d_val2.unwrap() != Decimal::new(0, 0) {
            assert_eq!(
                // Decimal + Float = Decimal
                evaluate_add(&Schema::default(), &mut dec1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 + d_val2.unwrap())
            );
            assert_eq!(
                // Decimal - Float = Decimal
                evaluate_sub(&Schema::default(), &mut dec1, &mut float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 - d_val2.unwrap())
            );
            // Decimal * Float = Decimal
            let res = evaluate_mul(&Schema::default(), &mut dec2, &mut float1, &row);
            if res.is_ok() {
                 assert_eq!(
                    res.unwrap(), Field::Decimal(d_num2.0 * d_val1.unwrap())
                );
            } else {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
                ));
            }
            // Decimal / Float = Decimal
            let res = evaluate_div(&Schema::default(), &mut dec2, &mut float1, &row);
            if d_num1.0 == Decimal::new(0, 0) {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
            else if res.is_ok() {
                 assert_eq!(
                    res.unwrap(), Field::Decimal(d_num2.0 / d_val1.unwrap())
                );
            } else {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
                ));
            }
            // Decimal % Float = Decimal
            let res = evaluate_mod(&Schema::default(), &mut dec1, &mut float2, &row);
            if d_num1.0 == Decimal::new(0, 0) {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
            else if res.is_ok() {
                 assert_eq!(
                    res.unwrap(),Field::Decimal(d_num1.0 % d_val2.unwrap())
                );
            } else {
                assert!(res.is_err());
                assert!(matches!(
                    res,
                    Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
                ));
            }
        }


        //// left: Decimal, right: Decimal
        assert_eq!(
            // Decimal + Decimal = Decimal
            evaluate_add(&Schema::default(), &mut dec1, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 + d_num2.0)
        );
        assert_eq!(
            // Decimal - Decimal = Decimal
            evaluate_sub(&Schema::default(), &mut dec1, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 - d_num2.0)
        );
        // Decimal * Decimal = Decimal
        let res = evaluate_mul(&Schema::default(), &mut dec2, &mut dec1, &row);
        if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num2.0 * d_num1.0)
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::MultiplicationOverflow))
            ));
        }
        // Decimal / Decimal = Decimal
        let res = evaluate_div(&Schema::default(), &mut dec2, &mut dec1, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num2.0 / d_num1.0)
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::DivisionByZeroOrOverflow))
            ));
        }
        // Decimal % Decimal = Decimal
        let res = evaluate_mod(&Schema::default(), &mut dec1, &mut dec2, &row);
        if d_num1.0 == Decimal::new(0, 0) {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }
        else if res.is_ok() {
             assert_eq!(
                res.unwrap(), Field::Decimal(d_num1.0 % d_num2.0)
            );
        } else {
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(PipelineError::SqlError(OperationError::ModuloByZeroOrOverflow))
            ));
        }

        //// left: Decimal, right: Null
        assert_eq!(
            // Decimal + Null = Null
            evaluate_add(&Schema::default(), &mut dec1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal - Null = Null
            evaluate_sub(&Schema::default(), &mut dec1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal * Null = Null
            evaluate_mul(&Schema::default(), &mut dec2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Null = Null
            evaluate_div(&Schema::default(), &mut dec2, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Null = Null
            evaluate_mod(&Schema::default(), &mut dec1, &mut null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    })
}

#[test]
fn test_null_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, u128_num1: u128, u128_num2: u128, i_num1: i64, i_num2: i64, i128_num1: i128, i128_num2: i128, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(vec![]);

        let mut uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let mut uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let mut u128_1 = Box::new(Literal(Field::U128(u128_num1)));
        let mut u128_2 = Box::new(Literal(Field::U128(u128_num2)));
        let mut int1 = Box::new(Literal(Field::Int(i_num1)));
        let mut int2 = Box::new(Literal(Field::Int(i_num2)));
        let mut i128_1 = Box::new(Literal(Field::I128(i128_num1)));
        let mut i128_2 = Box::new(Literal(Field::I128(i128_num2)));
        let mut float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let mut float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let mut dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let mut dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let mut null = Box::new(Literal(Field::Null));

        //// left: Null, right: UInt
        assert_eq!(
            // Null + UInt = Null
            evaluate_add(&Schema::default(), &mut null, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - UInt = Null
            evaluate_sub(&Schema::default(), &mut null, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * UInt = Null
            evaluate_mul(&Schema::default(), &mut null, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / UInt = Null
            evaluate_div(&Schema::default(), &mut null, &mut uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % UInt = Null
            evaluate_mod(&Schema::default(), &mut null, &mut uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: U128
        assert_eq!(
            // Null + U128 = Null
            evaluate_add(&Schema::default(), &mut null, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - U128 = Null
            evaluate_sub(&Schema::default(), &mut null, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * U128 = Null
            evaluate_mul(&Schema::default(), &mut null, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / U128 = Null
            evaluate_div(&Schema::default(), &mut null, &mut u128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % U128 = Null
            evaluate_mod(&Schema::default(), &mut null, &mut u128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Int
        assert_eq!(
            // Null + Int = Null
            evaluate_add(&Schema::default(), &mut null, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Int = Null
            evaluate_sub(&Schema::default(), &mut null, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Int = Null
            evaluate_mul(&Schema::default(), &mut null, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Int = Null
            evaluate_div(&Schema::default(), &mut null, &mut int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Int = Null
            evaluate_mod(&Schema::default(), &mut null, &mut int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: I128
        assert_eq!(
            // Null + I128 = Null
            evaluate_add(&Schema::default(), &mut null, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - I128 = Null
            evaluate_sub(&Schema::default(), &mut null, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * I128 = Null
            evaluate_mul(&Schema::default(), &mut null, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / I128 = Null
            evaluate_div(&Schema::default(), &mut null, &mut i128_1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % I128 = Null
            evaluate_mod(&Schema::default(), &mut null, &mut i128_2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Float
        assert_eq!(
            // Null + Float = Null
            evaluate_add(&Schema::default(), &mut null, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Float = Null
            evaluate_sub(&Schema::default(), &mut null, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Float = Null
            evaluate_mul(&Schema::default(), &mut null, &mut float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Float = Null
            evaluate_div(&Schema::default(), &mut null, &mut float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Float = Null
            evaluate_mod(&Schema::default(), &mut null, &mut float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Decimal
        assert_eq!(
            // Null + Decimal = Null
            evaluate_add(&Schema::default(), &mut null, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Decimal = Null
            evaluate_sub(&Schema::default(), &mut null, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Decimal = Null
            evaluate_mul(&Schema::default(), &mut null, &mut dec1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Decimal = Null
            evaluate_div(&Schema::default(), &mut null, &mut dec1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Decimal = Null
            evaluate_mod(&Schema::default(), &mut null, &mut dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Null
        let mut null_clone = null.clone();
        assert_eq!(
            // Null + Null = Null
            evaluate_add(&Schema::default(), &mut null, &mut null_clone, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Null = Null
            evaluate_sub(&Schema::default(), &mut null, &mut null_clone, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Null = Null
            evaluate_mul(&Schema::default(), &mut null, &mut null_clone, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Null = Null
            evaluate_div(&Schema::default(), &mut null, &mut null_clone, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Null = Null
            evaluate_mod(&Schema::default(), &mut null, &mut null_clone, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    })
}

#[test]
fn test_timestamp_difference() {
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                String::from("a"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                String::from("b"),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let record = Record::new(vec![
        Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap()),
        Field::Timestamp(DateTime::parse_from_rfc3339("2020-01-01T00:12:10Z").unwrap()),
    ]);

    let result = evaluate_sub(
        &schema,
        &mut Expression::Column { index: 0 },
        &mut Expression::Column { index: 1 },
        &record,
    )
    .unwrap();
    assert_eq!(
        result,
        Field::Duration(DozerDuration(
            std::time::Duration::from_nanos(50000 * 1000 * 1000),
            TimeUnit::Nanoseconds
        ))
    );

    let result = evaluate_sub(
        &schema,
        &mut Expression::Column { index: 1 },
        &mut Expression::Column { index: 0 },
        &record,
    );
    assert!(result.is_err());
}

#[test]
fn test_duration() {
    proptest!(
        ProptestConfig::with_cases(1000),
        move |(d1: u64, d2: u64, dt1: ArbitraryDateTime)| {
            test_duration_math(d1, d2, dt1)
    });
}

fn test_duration_math(d1: u64, d2: u64, dt1: ArbitraryDateTime) {
    let row = Record::new(vec![]);

    let mut v = Expression::Literal(Field::Date(dt1.0.date_naive()));
    let mut dur1 = Expression::Literal(Field::Duration(DozerDuration(
        std::time::Duration::from_nanos(d1),
        TimeUnit::Nanoseconds,
    )));
    let mut dur2 = Expression::Literal(Field::Duration(DozerDuration(
        std::time::Duration::from_nanos(d2),
        TimeUnit::Nanoseconds,
    )));

    // Duration + Duration = Duration
    let result = evaluate_add(&Schema::default(), &mut dur1, &mut dur2, &row);
    let sum = std::time::Duration::from_nanos(d1).checked_add(std::time::Duration::from_nanos(d2));
    if result.is_ok() && sum.is_some() {
        assert_eq!(
            result.unwrap(),
            Field::Duration(DozerDuration(sum.unwrap(), TimeUnit::Nanoseconds))
        );
    }
    // Duration - Duration = Duration
    let result = evaluate_sub(&Schema::default(), &mut dur1, &mut dur2, &row);
    let diff = std::time::Duration::from_nanos(d1).checked_sub(std::time::Duration::from_nanos(d2));
    if result.is_ok() && diff.is_some() {
        assert_eq!(
            result.unwrap(),
            Field::Duration(DozerDuration(diff.unwrap(), TimeUnit::Nanoseconds))
        );
    }
    // Duration * Duration = Error
    let result = evaluate_mul(&Schema::default(), &mut dur1, &mut dur2, &row);
    assert!(result.is_err());
    // Duration / Duration = Error
    let result = evaluate_div(&Schema::default(), &mut dur1, &mut dur2, &row);
    assert!(result.is_err());
    // Duration % Duration = Error
    let result = evaluate_mod(&Schema::default(), &mut dur1, &mut dur2, &row);
    assert!(result.is_err());

    // Duration + Timestamp = Error
    let result = evaluate_add(&Schema::default(), &mut dur1, &mut v, &row);
    assert!(result.is_err());
    // Duration - Timestamp = Error
    let result = evaluate_sub(&Schema::default(), &mut dur1, &mut v, &row);
    assert!(result.is_err());
    // Duration * Timestamp = Error
    let result = evaluate_mul(&Schema::default(), &mut dur1, &mut v, &row);
    assert!(result.is_err());
    // Duration / Timestamp = Error
    let result = evaluate_div(&Schema::default(), &mut dur1, &mut v, &row);
    assert!(result.is_err());
    // Duration % Timestamp = Error
    let result = evaluate_mod(&Schema::default(), &mut dur1, &mut v, &row);
    assert!(result.is_err());

    // Timestamp + Duration = Timestamp
    let result = evaluate_add(&Schema::default(), &mut v, &mut dur1, &row);
    let sum = dt1
        .0
        .checked_add_signed(chrono::Duration::nanoseconds(d1 as i64));
    if result.is_ok() && sum.is_some() {
        assert_eq!(result.unwrap(), Field::Timestamp(sum.unwrap()));
    }
    // Timestamp - Duration = Timestamp
    let result = evaluate_sub(&Schema::default(), &mut v, &mut dur2, &row);
    let diff = dt1
        .0
        .checked_sub_signed(chrono::Duration::nanoseconds(d2 as i64));
    if result.is_ok() && diff.is_some() {
        assert_eq!(result.unwrap(), Field::Timestamp(diff.unwrap()));
    }
    // Timestamp * Duration = Error
    let result = evaluate_mul(&Schema::default(), &mut v, &mut dur1, &row);
    assert!(result.is_err());
    // Timestamp / Duration = Error
    let result = evaluate_div(&Schema::default(), &mut v, &mut dur1, &row);
    assert!(result.is_err());
    // Timestamp % Duration = Error
    let result = evaluate_mod(&Schema::default(), &mut v, &mut dur1, &row);
    assert!(result.is_err());
}

#[test]
fn test_decimal() {
    let mut dec1 = Box::new(Literal(Field::Decimal(Decimal::from_i64(1_i64).unwrap())));
    let mut dec2 = Box::new(Literal(Field::Decimal(Decimal::from_i64(2_i64).unwrap())));
    let mut float1 = Box::new(Literal(Field::Float(
        OrderedFloat::<f64>::from_i64(1_i64).unwrap(),
    )));
    let mut float2 = Box::new(Literal(Field::Float(
        OrderedFloat::<f64>::from_i64(2_i64).unwrap(),
    )));
    let mut int1 = Box::new(Literal(Field::Int(1_i64)));
    let mut int2 = Box::new(Literal(Field::Int(2_i64)));
    let mut uint1 = Box::new(Literal(Field::UInt(1_u64)));
    let mut uint2 = Box::new(Literal(Field::UInt(2_u64)));

    let row = Record::new(vec![]);

    // left: Int, right: Decimal
    assert_eq!(
        evaluate_add(&Schema::default(), &mut int1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(2_i64).unwrap())
    );
    assert_eq!(
        evaluate_sub(&Schema::default(), &mut int1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(0_i64).unwrap())
    );
    assert_eq!(
        evaluate_mul(&Schema::default(), &mut int2, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(2_i64).unwrap())
    );
    assert_eq!(
        evaluate_div(&Schema::default(), &mut int1, dec2.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_f64(0.5).unwrap())
    );
    assert_eq!(
        evaluate_mod(&Schema::default(), &mut int1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(0_i64).unwrap())
    );

    // left: UInt, right: Decimal
    assert_eq!(
        evaluate_add(&Schema::default(), &mut uint1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(2_i64).unwrap())
    );
    assert_eq!(
        evaluate_sub(&Schema::default(), &mut uint1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(0_i64).unwrap())
    );
    assert_eq!(
        evaluate_mul(&Schema::default(), &mut uint2, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(2_i64).unwrap())
    );
    assert_eq!(
        evaluate_div(&Schema::default(), &mut uint1, dec2.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_f64(0.5).unwrap())
    );
    assert_eq!(
        evaluate_mod(&Schema::default(), &mut uint1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(0_i64).unwrap())
    );

    // left: Float, right: Decimal
    assert_eq!(
        evaluate_add(&Schema::default(), &mut float1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(2_i64).unwrap())
    );
    assert_eq!(
        evaluate_sub(&Schema::default(), &mut float1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(0_i64).unwrap())
    );
    assert_eq!(
        evaluate_mul(&Schema::default(), &mut float2, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(2_i64).unwrap())
    );
    assert_eq!(
        evaluate_div(&Schema::default(), &mut float1, dec2.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_f64(0.5).unwrap())
    );
    assert_eq!(
        evaluate_mod(&Schema::default(), &mut float1, dec1.as_mut(), &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Decimal(Decimal::from_i64(0_i64).unwrap())
    );
}
