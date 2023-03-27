use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::mathematical::{
    evaluate_add, evaluate_div, evaluate_mod, evaluate_mul, evaluate_sub,
};
use dozer_types::types::Record;
use dozer_types::{
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    types::{Field, Schema},
};
use num_traits::FromPrimitive;

use proptest::prelude::*;
use std::num::Wrapping;

#[derive(Debug)]
struct ArbitraryDecimal(Decimal);

impl Arbitrary for ArbitraryDecimal {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (i64::MIN..i64::MAX, u32::MIN..29u32)
            .prop_map(|(num, scale)| ArbitraryDecimal(Decimal::new(num, scale)))
            .boxed()
    }
}

#[test]
fn test_uint_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(None, vec![], None);

        let uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let int1 = Box::new(Literal(Field::Int(i_num1)));
        let int2 = Box::new(Literal(Field::Int(i_num2)));
        let float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let _dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let null = Box::new(Literal(Field::Null));

        //// left: UInt, right: UInt
        assert_eq!(
            // UInt + UInt = UInt
            evaluate_add(&Schema::empty(), &uint1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num1) + Wrapping(u_num2)).0)
        );
        assert_eq!(
            // UInt - UInt = UInt
            evaluate_sub(&Schema::empty(), &uint1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num1) - Wrapping(u_num2)).0)
        );
        assert_eq!(
            // UInt * UInt = UInt
            evaluate_mul(&Schema::empty(), &uint2, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num2) * Wrapping(u_num1)).0)
        );
        assert_eq!(
            // UInt / UInt = Float
            evaluate_div(&Schema::empty(), &uint2, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // UInt % UInt = UInt
            evaluate_mod(&Schema::empty(), &uint1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::UInt((Wrapping(u_num1) % Wrapping(u_num2)).0)
        );

        //// left: UInt, right: Int
        assert_eq!(
            // UInt + Int = Int
            evaluate_add(&Schema::empty(), &uint1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num1 as i64) + Wrapping(i_num2)).0)
        );
        assert_eq!(
            // UInt - Int = Int
            evaluate_sub(&Schema::empty(), &uint1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num1 as i64) - Wrapping(i_num2)).0)
        );
        assert_eq!(
            // UInt * Int = Int
            evaluate_mul(&Schema::empty(), &uint2, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num2 as i64) * Wrapping(i_num1)).0)
        );
        assert_eq!(
            // UInt / Int = Float
            evaluate_div(&Schema::empty(), &uint2, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // UInt % Int = Int
            evaluate_mod(&Schema::empty(), &uint1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(u_num1 as i64) % Wrapping(i_num2)).0)
        );

        //// left: UInt, right: Float
        assert_eq!(
            // UInt + Float = Float
            evaluate_add(&Schema::empty(), &uint1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() + f_num2))
        );
        assert_eq!(
            // UInt - Float = Float
            evaluate_sub(&Schema::empty(), &uint1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() - f_num2))
        );
        assert_eq!(
            // UInt * Float = Float
            evaluate_mul(&Schema::empty(), &uint2, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() * f_num1))
        );
        assert_eq!(
            // UInt / Float = Float
            evaluate_div(&Schema::empty(), &uint2, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f_num1))
        );
        assert_eq!(
            // UInt % Float = Float
            evaluate_mod(&Schema::empty(), &uint1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() % f_num2))
        );

        //// left: UInt, right: Decimal
        assert_eq!(
            // UInt + Decimal = Decimal
            evaluate_add(&Schema::empty(), &uint1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() + d_num2.0)
        );
        assert_eq!(
            // UInt - Decimal = Decimal
            evaluate_sub(&Schema::empty(), &uint1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() - d_num2.0)
        );
        // // todo: Multiplication overflowed
        // assert_eq!(
        //     // UInt * Decimal = Decimal
        //     evaluate_mul(&Schema::empty(), &uint2, &dec1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(Decimal::from_u64(u_num2).unwrap().checked_mul(d_num1.0).unwrap())
        // );
        // // todo: Division overflowed
        // assert_eq!(
        //     // UInt / Decimal = Decimal
        //     evaluate_div(&Schema::empty(), &uint2, &dec1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(Decimal::from_u64(u_num2).unwrap() / d_num1.0)
        // );
        assert_eq!(
            // UInt % Decimal = Decimal
            evaluate_mod(&Schema::empty(), &uint1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() % d_num2.0)
        );

        //// left: UInt, right: Null
        assert_eq!(
            // UInt + Null = Null
            evaluate_add(&Schema::empty(), &uint1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt - Null = Null
            evaluate_sub(&Schema::empty(), &uint1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt * Null = Null
            evaluate_mul(&Schema::empty(), &uint2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt / Null = Null
            evaluate_div(&Schema::empty(), &uint2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // UInt % Null = Null
            evaluate_mod(&Schema::empty(), &uint1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_int_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(None, vec![], None);

        let uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let int1 = Box::new(Literal(Field::Int(i_num1)));
        let int2 = Box::new(Literal(Field::Int(i_num2)));
        let float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let _dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let null = Box::new(Literal(Field::Null));

        //// left: Int, right: UInt
        assert_eq!(
            // Int + UInt = Int
            evaluate_add(&Schema::empty(), &int1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) + Wrapping(u_num2 as i64)).0)
        );
        assert_eq!(
            // Int - UInt = Int
            evaluate_sub(&Schema::empty(), &int1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) - Wrapping(u_num2 as i64)).0)
        );
        assert_eq!(
            // Int * UInt = Int
            evaluate_mul(&Schema::empty(), &int2, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num2) * Wrapping(u_num1 as i64)).0)
        );
        assert_eq!(
            // Int / UInt = Float
            evaluate_div(&Schema::empty(), &int2, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() / f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // Int % UInt = Int
            evaluate_mod(&Schema::empty(), &int1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) % Wrapping(u_num2 as i64)).0)
        );

        //// left: Int, right: Int
        assert_eq!(
            // Int + Int = Int
            evaluate_add(&Schema::empty(), &int1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) + Wrapping(i_num2)).0)
        );
        assert_eq!(
            // Int - Int = Int
            evaluate_sub(&Schema::empty(), &int1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) - Wrapping(i_num2)).0)
        );
        assert_eq!(
            // Int * Int = Int
            evaluate_mul(&Schema::empty(), &int2, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num2) * Wrapping(i_num1)).0)
        );
        assert_eq!(
            // Int / Int = Float
            evaluate_div(&Schema::empty(), &int2, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() / f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // Int % Int = Int
            evaluate_mod(&Schema::empty(), &int1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Int((Wrapping(i_num1) % Wrapping(i_num2)).0)
        );

        //// left: Int, right: Float
        assert_eq!(
            // Int + Float = Float
            evaluate_add(&Schema::empty(), &int1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num1).unwrap() + f_num2))
        );
        assert_eq!(
            // Int - Float = Float
            evaluate_sub(&Schema::empty(), &int1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num1).unwrap() - f_num2))
        );
        assert_eq!(
            // Int * Float = Float
            evaluate_mul(&Schema::empty(), &int2, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() * f_num1))
        );
        assert_eq!(
            // Int / Float = Float
            evaluate_div(&Schema::empty(), &int2, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num2).unwrap() / f_num1))
        );
        assert_eq!(
            // Int % Float = Float
            evaluate_mod(&Schema::empty(), &int1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f64::from_i64(i_num1).unwrap() % f_num2))
        );

        //// left: Int, right: Decimal
        assert_eq!(
            // Int + Decimal = Decimal
            evaluate_add(&Schema::empty(), &int1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_i64(i_num1).unwrap() + d_num2.0)
        );
        assert_eq!(
            // Int - Decimal = Decimal
            evaluate_sub(&Schema::empty(), &int1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_i64(i_num1).unwrap() - d_num2.0)
        );
        // // todo: Multiplication overflowed
        // assert_eq!(
        //     // Int * Decimal = Decimal
        //     evaluate_mul(&Schema::empty(), &int2, &dec1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(Decimal::from_i64(i_num2).unwrap() *d_num1.0)
        // );
        // // todo: Division overflowed
        // assert_eq!(
        //     // Int / Decimal = Decimal
        //     evaluate_div(&Schema::empty(), &int2, &dec1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(Decimal::from_i64(i_num2).unwrap() / d_num1.0)
        // );
        assert_eq!(
            // Int % Decimal = Decimal
            evaluate_mod(&Schema::empty(), &int1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_i64(i_num1).unwrap() % d_num2.0)
        );

        //// left: Int, right: Null
        assert_eq!(
            // Int + Null = Null
            evaluate_add(&Schema::empty(), &int1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int - Null = Null
            evaluate_sub(&Schema::empty(), &int1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int * Null = Null
            evaluate_mul(&Schema::empty(), &int2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int / Null = Null
            evaluate_div(&Schema::empty(), &int2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Int % Null = Null
            evaluate_mod(&Schema::empty(), &int1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_float_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(None, vec![], None);

        let uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let int1 = Box::new(Literal(Field::Int(i_num1)));
        let int2 = Box::new(Literal(Field::Int(i_num2)));
        let float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let _dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let null = Box::new(Literal(Field::Null));

        //// left: Float, right: UInt
        assert_eq!(
            // Float + UInt = Float
            evaluate_add(&Schema::empty(), &float1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) + OrderedFloat(f64::from_u64(u_num2).unwrap()))
        );
        assert_eq!(
            // Float - UInt = Float
            evaluate_sub(&Schema::empty(), &float1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) - OrderedFloat(f64::from_u64(u_num2).unwrap()))
        );
        assert_eq!(
            // Float * UInt = Float
            evaluate_mul(&Schema::empty(), &float2, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) * OrderedFloat(f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // Float / UInt = Float
            evaluate_div(&Schema::empty(), &float2, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) / OrderedFloat(f64::from_u64(u_num1).unwrap()))
        );
        assert_eq!(
            // Float % UInt = Float
            evaluate_mod(&Schema::empty(), &float1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) % OrderedFloat(f64::from_u64(u_num2).unwrap()))
        );

        //// left: Float, right: Int
        assert_eq!(
            // Float + Int = Float
            evaluate_add(&Schema::empty(), &float1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) + OrderedFloat(f64::from_i64(i_num2).unwrap()))
        );
        assert_eq!(
            // Float - Int = Float
            evaluate_sub(&Schema::empty(), &float1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) - OrderedFloat(f64::from_i64(i_num2).unwrap()))
        );
        assert_eq!(
            // Float * Int = Float
            evaluate_mul(&Schema::empty(), &float2, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) * OrderedFloat(f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // Float / Int = Float
            evaluate_div(&Schema::empty(), &float2, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2) / OrderedFloat(f64::from_i64(i_num1).unwrap()))
        );
        assert_eq!(
            // Float % Int = Float
            evaluate_mod(&Schema::empty(), &float1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1) % OrderedFloat(f64::from_i64(i_num2).unwrap()))
        );

        //// left: Float, right: Float
        assert_eq!(
            // Float + Float = Float
            evaluate_add(&Schema::empty(), &float1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1 + f_num2))
        );
        assert_eq!(
            // Float - Float = Float
            evaluate_sub(&Schema::empty(), &float1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1 - f_num2))
        );
        assert_eq!(
            // Float * Float = Float
            evaluate_mul(&Schema::empty(), &float2, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2 * f_num1))
        );
        assert_eq!(
            // Float / Float = Float
            evaluate_div(&Schema::empty(), &float2, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num2 / f_num1))
        );
        assert_eq!(
            // Float % Float = Float
            evaluate_mod(&Schema::empty(), &float1, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Float(OrderedFloat(f_num1 % f_num2))
        );

        //// left: Float, right: Decimal
        let d_val1 = Decimal::from_f64(f_num1);
        let d_val2 = Decimal::from_f64(f_num2);
        if d_val1.is_some() && d_val2.is_some() {
            assert_eq!(
                // Float + Decimal = Decimal
                evaluate_add(&Schema::empty(), &float1, &dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_val1.unwrap() + d_num2.0)
            );
            assert_eq!(
                // Float - Decimal = Decimal
                evaluate_sub(&Schema::empty(), &float1, &dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_val1.unwrap() - d_num2.0)
            );
            // // todo: Multiplication overflowed
            // assert_eq!(
            //     // Float * Decimal = Decimal
            //     evaluate_mul(&Schema::empty(), &float2, &dec1, &row)
            //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
            //     Field::Decimal(d_val2.unwrap() * d_num1.0)
            // );
            // // todo: Division overflowed
            // assert_eq!(
            //     // Float / Decimal = Decimal
            //     evaluate_div(&Schema::empty(), &float2, &dec1, &row)
            //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
            //     Field::Decimal(d_val2.unwrap() / d_num1.0)
            // );
            assert_eq!(
                // Float % Decimal = Decimal
                evaluate_mod(&Schema::empty(), &float1, &dec2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_val1.unwrap() % d_num2.0)
            );
        }

        //// left: Float, right: Null
        assert_eq!(
            // Float + Null = Null
            evaluate_add(&Schema::empty(), &float1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float - Null = Null
            evaluate_sub(&Schema::empty(), &float1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float * Null = Null
            evaluate_mul(&Schema::empty(), &float2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float / Null = Null
            evaluate_div(&Schema::empty(), &float2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Float % Null = Null
            evaluate_mod(&Schema::empty(), &float1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    });
}

#[test]
fn test_decimal_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(None, vec![], None);

        let uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let int1 = Box::new(Literal(Field::Int(i_num1)));
        let int2 = Box::new(Literal(Field::Int(i_num2)));
        let _float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let null = Box::new(Literal(Field::Null));

        //// left: Decimal, right: UInt
        assert_eq!(
            // Decimal + UInt = Decimal
            evaluate_add(&Schema::empty(), &dec1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 + Decimal::from(u_num2))
        );
        assert_eq!(
            // Decimal - UInt = Decimal
            evaluate_sub(&Schema::empty(), &dec1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 - Decimal::from(u_num2))
        );
        // // todo: Multiplication overflowed
        // assert_eq!(
        //     // Decimal * UInt = Decimal
        //     evaluate_mul(&Schema::empty(), &dec2, &uint1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(d_num2.0 * Decimal::from(u_num1))
        // );
        assert_eq!(
            // Decimal / UInt = Decimal
            evaluate_div(&Schema::empty(), &dec2, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num2.0 / Decimal::from(u_num1))
        );
        assert_eq!(
            // Decimal % UInt = Decimal
            evaluate_mod(&Schema::empty(), &dec1, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 % Decimal::from(u_num2))
        );

        //// left: Decimal, right: Int
        assert_eq!(
            // Decimal + Int = Decimal
            evaluate_add(&Schema::empty(), &dec1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 + Decimal::from(i_num2))
        );
        assert_eq!(
            // Decimal - Int = Decimal
            evaluate_sub(&Schema::empty(), &dec1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 - Decimal::from(i_num2))
        );
        // // todo: Multiplication overflowed
        // assert_eq!(
        //     // Decimal * Int = Float
        //     evaluate_mul(&Schema::empty(), &dec2, &int1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(d_num2.0 * Decimal::from(i_num1))
        // );
        assert_eq!(
            // Decimal / Int = Decimal
            evaluate_div(&Schema::empty(), &dec2, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num2.0 / Decimal::from(i_num1))
        );
        assert_eq!(
            // Decimal % Int = Decimal
            evaluate_mod(&Schema::empty(), &dec1, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 % Decimal::from(i_num2))
        );

        // left: Decimal, right: Float
        let d_val1 = Decimal::from_f64(f_num1);
        let d_val2 = Decimal::from_f64(f_num2);
        if d_val1.is_some() && d_val2.is_some() && d_val1.unwrap() != Decimal::new(0, 0) && d_val2.unwrap() != Decimal::new(0, 0) {
            assert_eq!(
                // Decimal + Float = Decimal
                evaluate_add(&Schema::empty(), &dec1, &float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 + d_val2.unwrap())
            );
            assert_eq!(
                // Decimal - Float = Decimal
                evaluate_sub(&Schema::empty(), &dec1, &float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 - d_val2.unwrap())
            );
            // // todo: Multiplication overflowed
            // assert_eq!(
            //     // Decimal * Float = Decimal
            //     evaluate_mul(&Schema::empty(), &dec2, &float1, &row)
            //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
            //     Field::Decimal(d_num2.0 * d_val1.unwrap())
            // );
            // // todo: Division overflowed
            // assert_eq!(
            //     // Decimal / Float = Decimal
            //     evaluate_div(&Schema::empty(), &dec2, &float1, &row)
            //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
            //     Field::Decimal(d_num2.0 / d_val1.unwrap())
            // );
            assert_eq!(
                // Decimal % Float = Decimal
                evaluate_mod(&Schema::empty(), &dec1, &float2, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Decimal(d_num1.0 % d_val2.unwrap())
            );
        }


        //// left: Decimal, right: Decimal
        assert_eq!(
            // Decimal + Decimal = Decimal
            evaluate_add(&Schema::empty(), &dec1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 + d_num2.0)
        );
        assert_eq!(
            // Decimal - Decimal = Decimal
            evaluate_sub(&Schema::empty(), &dec1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 - d_num2.0)
        );
        // // todo: Multiplication overflowed
        // assert_eq!(
        //     // Decimal * Decimal = Decimal
        //     evaluate_mul(&Schema::empty(), &dec2, &dec1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(d_num2.0 * d_num1.0)
        // );
        assert_eq!(
            // Decimal / Decimal = Decimal
            evaluate_div(&Schema::empty(), &dec2, &dec1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num2.0 / d_num1.0)
        );
        assert_eq!(
            // Decimal % Decimal = Decimal
            evaluate_mod(&Schema::empty(), &dec1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(d_num1.0 % d_num2.0)
        );

        //// left: Decimal, right: Null
        assert_eq!(
            // Decimal + Null = Null
            evaluate_add(&Schema::empty(), &dec1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal - Null = Null
            evaluate_sub(&Schema::empty(), &dec1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal * Null = Null
            evaluate_mul(&Schema::empty(), &dec2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Null = Null
            evaluate_div(&Schema::empty(), &dec2, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Null = Null
            evaluate_mod(&Schema::empty(), &dec1, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    })
}

#[test]
fn test_null_math() {
    proptest!(ProptestConfig::with_cases(1000), move |(u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64, d_num1: ArbitraryDecimal, d_num2: ArbitraryDecimal)| {
        let row = Record::new(None, vec![], None);

        let uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let int1 = Box::new(Literal(Field::Int(i_num1)));
        let int2 = Box::new(Literal(Field::Int(i_num2)));
        let float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let dec1 = Box::new(Literal(Field::Decimal(d_num1.0)));
        let dec2 = Box::new(Literal(Field::Decimal(d_num2.0)));

        let null = Box::new(Literal(Field::Null));

        //// left: Null, right: UInt
        assert_eq!(
            // Null + UInt = Null
            evaluate_add(&Schema::empty(), &null, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - UInt = Null
            evaluate_sub(&Schema::empty(), &null, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * UInt = Null
            evaluate_mul(&Schema::empty(), &null, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / UInt = Null
            evaluate_div(&Schema::empty(), &null, &uint1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % UInt = Null
            evaluate_mod(&Schema::empty(), &null, &uint2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Int
        assert_eq!(
            // Null + Int = Null
            evaluate_add(&Schema::empty(), &null, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Int = Null
            evaluate_sub(&Schema::empty(), &null, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Int = Null
            evaluate_mul(&Schema::empty(), &null, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Int = Null
            evaluate_div(&Schema::empty(), &null, &int1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Int = Null
            evaluate_mod(&Schema::empty(), &null, &int2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Float
        assert_eq!(
            // Null + Float = Null
            evaluate_add(&Schema::empty(), &null, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Float = Null
            evaluate_sub(&Schema::empty(), &null, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Float = Null
            evaluate_mul(&Schema::empty(), &null, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Float = Null
            evaluate_div(&Schema::empty(), &null, &float1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Float = Null
            evaluate_mod(&Schema::empty(), &null, &float2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Decimal
        assert_eq!(
            // Null + Decimal = Null
            evaluate_add(&Schema::empty(), &null, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Decimal = Null
            evaluate_sub(&Schema::empty(), &null, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Decimal = Null
            evaluate_mul(&Schema::empty(), &null, &dec1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Decimal = Null
            evaluate_div(&Schema::empty(), &null, &dec1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Decimal = Null
            evaluate_mod(&Schema::empty(), &null, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );

        //// left: Null, right: Null
        assert_eq!(
            // Null + Null = Null
            evaluate_add(&Schema::empty(), &null, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null - Null = Null
            evaluate_sub(&Schema::empty(), &null, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Null * Null = Null
            evaluate_mul(&Schema::empty(), &null, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal / Null = Null
            evaluate_div(&Schema::empty(), &null, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
        assert_eq!(
            // Decimal % Null = Null
            evaluate_mod(&Schema::empty(), &null, &null, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Null
        );
    })
}
