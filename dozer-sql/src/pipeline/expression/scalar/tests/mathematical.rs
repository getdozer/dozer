use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::mathematical::{
    evaluate_add, evaluate_div, evaluate_mod, evaluate_mul, evaluate_sub,
};
use dozer_types::types::{Record, SourceDefinition};
use dozer_types::{
    chrono::{DateTime, NaiveDate, TimeZone, Utc},
    ordered_float::OrderedFloat,
    rust_decimal::Decimal,
    types::{Field, FieldDefinition, FieldType, Schema},
};
use num_traits::FromPrimitive;

use proptest::prelude::*;
use std::num::Wrapping;

// #[test]
// fn test_uint_math() {
//     proptest!(ProptestConfig::with_cases(100), move |(u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64)| {
//         let row = Record::new(None, vec![], None);
//
//         let uint1 = Box::new(Literal(Field::UInt(u_num1)));
//         let uint2 = Box::new(Literal(Field::UInt(u_num2)));
//         let int1 = Box::new(Literal(Field::Int(i_num1)));
//         let int2 = Box::new(Literal(Field::Int(i_num2)));
//         let float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
//         let float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
//         let dec1 = Box::new(Literal(Field::Decimal(Decimal::from(u_num1))));
//         let dec2 = Box::new(Literal(Field::Decimal(Decimal::from(u_num2))));
//
//         //// left: UInt, right: UInt
//         assert_eq!(
//             // UInt + UInt = UInt
//             evaluate_add(&Schema::empty(), &uint1, &uint2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::UInt((Wrapping(u_num1) + Wrapping(u_num2)).0)
//         );
//         assert_eq!(
//             // UInt - UInt = UInt
//             evaluate_sub(&Schema::empty(), &uint1, &uint2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::UInt((Wrapping(u_num1) - Wrapping(u_num2)).0)
//         );
//         assert_eq!(
//             // UInt * UInt = UInt
//             evaluate_mul(&Schema::empty(), &uint2, &uint1, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::UInt((Wrapping(u_num2) * Wrapping(u_num1)).0)
//         );
//         assert_eq!(
//             // UInt / UInt = Float
//             evaluate_div(&Schema::empty(), &uint2, &uint1, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_u64(u_num1).unwrap()))
//         );
//         assert_eq!(
//             // UInt % UInt = UInt
//             evaluate_mod(&Schema::empty(), &uint1, &uint2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::UInt((Wrapping(u_num1) % Wrapping(u_num2)).0)
//         );
//
//         //// left: UInt, right: Int
//         assert_eq!(
//             // UInt + Int = Int
//             evaluate_add(&Schema::empty(), &uint1, &int2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Int((Wrapping(u_num1 as i64) + Wrapping(i_num2)).0)
//         );
//         assert_eq!(
//             // UInt - Int = Int
//             evaluate_sub(&Schema::empty(), &uint1, &int2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Int((Wrapping(u_num1 as i64) - Wrapping(i_num2)).0)
//         );
//         assert_eq!(
//             // UInt * Int = Int
//             evaluate_mul(&Schema::empty(), &uint2, &int1, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Int((Wrapping(u_num2 as i64) * Wrapping(i_num1)).0)
//         );
//         assert_eq!(
//             // UInt / Int = Float
//             evaluate_div(&Schema::empty(), &uint2, &int1, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f64::from_i64(i_num1).unwrap()))
//         );
//         assert_eq!(
//             // UInt % Int = Int
//             evaluate_mod(&Schema::empty(), &uint1, &int2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Int((Wrapping(u_num1 as i64) % Wrapping(i_num2)).0)
//         );
//
//         //// left: UInt, right: Float
//         assert_eq!(
//             // UInt + Float = Float
//             evaluate_add(&Schema::empty(), &uint1, &float2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() + f_num2))
//         );
//         assert_eq!(
//             // UInt - Float = Float
//             evaluate_sub(&Schema::empty(), &uint1, &float2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() - f_num2))
//         );
//         assert_eq!(
//             // UInt * Float = Float
//             evaluate_mul(&Schema::empty(), &uint2, &float1, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() * f_num1))
//         );
//         assert_eq!(
//             // UInt / Float = Float
//             evaluate_div(&Schema::empty(), &uint2, &float1, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Float(OrderedFloat(f64::from_u64(u_num2).unwrap() / f_num1))
//         );
//         assert_eq!(
//             // UInt % Float = Float
//             evaluate_mod(&Schema::empty(), &uint1, &float2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Float(OrderedFloat(f64::from_u64(u_num1).unwrap() % f_num2))
//         );
//
//         //// left: UInt, right: Decimal
//         assert_eq!(
//             // UInt + Decimal = Decimal
//             evaluate_add(&Schema::empty(), &uint1, &dec2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Decimal(Decimal::from_u64(u_num1).unwrap() + Decimal::from_u64(u_num2).unwrap())
//         );
//         assert_eq!(
//             // UInt - Decimal = Decimal
//             evaluate_sub(&Schema::empty(), &uint1, &dec2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Decimal(Decimal::from_u64(u_num1).unwrap() - Decimal::from_u64(u_num2).unwrap())
//         );
//         //// todo: Multiplication overflowed
//         // assert_eq!(
//         //     // UInt * Decimal = Decimal
//         //     evaluate_mul(&Schema::empty(), &uint2, &dec1, &row)
//         //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
//         //     Field::Decimal(Decimal::from_u64(u_num2).unwrap() * Decimal::from_u64(u_num1).unwrap())
//         // );
//         assert_eq!(
//             // UInt / Decimal = Decimal
//             evaluate_div(&Schema::empty(), &uint2, &dec1, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Decimal(Decimal::from_u64(u_num2).unwrap() / Decimal::from_u64(u_num1).unwrap())
//         );
//         assert_eq!(
//             // UInt % Decimal = Decimal
//             evaluate_mod(&Schema::empty(), &uint1, &dec2, &row)
//                 .unwrap_or_else(|e| panic!("{}", e.to_string())),
//             Field::Decimal(Decimal::from_u64(u_num1).unwrap() % Decimal::from_u64(u_num2).unwrap())
//         );
//     });
// }

#[test]
fn test_int_math() {
    proptest!(ProptestConfig::with_cases(100), move |(u_num1: u64, u_num2: u64, i_num1: i64, i_num2: i64, f_num1: f64, f_num2: f64)| {
        let row = Record::new(None, vec![], None);

        let uint1 = Box::new(Literal(Field::UInt(u_num1)));
        let uint2 = Box::new(Literal(Field::UInt(u_num2)));
        let int1 = Box::new(Literal(Field::Int(i_num1)));
        let int2 = Box::new(Literal(Field::Int(i_num2)));
        let float1 = Box::new(Literal(Field::Float(OrderedFloat(f_num1))));
        let float2 = Box::new(Literal(Field::Float(OrderedFloat(f_num2))));
        let dec1 = Box::new(Literal(Field::Decimal(Decimal::from(u_num1))));
        let dec2 = Box::new(Literal(Field::Decimal(Decimal::from(u_num2))));

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
            Field::Int((Wrapping(i_num2) * Wrapping(u_num2 as i64)).0)
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
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() + Decimal::from_u64(u_num2).unwrap())
        );
        assert_eq!(
            // UInt - Decimal = Decimal
            evaluate_sub(&Schema::empty(), &uint1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() - Decimal::from_u64(u_num2).unwrap())
        );
        //// todo: Multiplication overflowed
        // assert_eq!(
        //     // UInt * Decimal = Decimal
        //     evaluate_mul(&Schema::empty(), &uint2, &dec1, &row)
        //         .unwrap_or_else(|e| panic!("{}", e.to_string())),
        //     Field::Decimal(Decimal::from_u64(u_num2).unwrap() * Decimal::from_u64(u_num1).unwrap())
        // );
        assert_eq!(
            // UInt / Decimal = Decimal
            evaluate_div(&Schema::empty(), &uint2, &dec1, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num2).unwrap() / Decimal::from_u64(u_num1).unwrap())
        );
        assert_eq!(
            // UInt % Decimal = Decimal
            evaluate_mod(&Schema::empty(), &uint1, &dec2, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Decimal(Decimal::from_u64(u_num1).unwrap() % Decimal::from_u64(u_num2).unwrap())
        );
    });
}
