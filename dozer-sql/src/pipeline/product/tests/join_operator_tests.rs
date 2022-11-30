use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, Record},
};

use crate::pipeline::product::join::get_join_key;

#[test]
fn test_composite_key() {
    let join_key = get_join_key(
        &Record::new(
            None,
            vec![
                Field::Int(11),
                Field::String("Alice".to_string()),
                Field::Int(0),
                Field::Float(OrderedFloat(5.5)),
            ],
        ),
        &[0_usize, 1_usize],
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(
        join_key,
        vec![
            0_u8, 13_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 11_u8, 65_u8, 108_u8, 105_u8,
            99_u8, 101_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8
        ]
    );
}
