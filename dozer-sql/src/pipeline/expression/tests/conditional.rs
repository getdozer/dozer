use crate::pipeline::expression::conditional::*;
use crate::pipeline::expression::execution::Expression;
use crate::pipeline::expression::tests::test_common::*;
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};
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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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
        let mut row = ProcessorRecord::new();
        row.push(f.clone());

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

fn test_evaluate_coalesce(
    args: &[Expression],
    row: &ProcessorRecord,
    typ: FieldType,
    _result: Field,
) {
    let schema = Schema::default()
        .field(
            FieldDefinition::new(String::from("field"), typ, false, SourceDefinition::Dynamic),
            false,
        )
        .clone();

    let res = evaluate_coalesce(&schema, args, row).unwrap();
    assert_eq!(res, _result);
}

#[test]
fn test_coalesce_logic() {
    let f = run_fct(
        "SELECT COALESCE(field, 2) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::Int(2));

    let f = run_fct(
        "SELECT COALESCE(field, CAST(2 AS FLOAT)) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::Float(OrderedFloat(2.0)));

    let f = run_fct(
        "SELECT COALESCE(field, 'X') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::String("X".to_string()));

    let f = run_fct(
        "SELECT COALESCE(field, 'X') FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::String("X".to_string()));
}

#[test]
fn test_coalesce_logic_null() {
    let f = run_fct(
        "SELECT COALESCE(field) FROM users",
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("field"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Null],
    );
    assert_eq!(f, Field::Null);
}
