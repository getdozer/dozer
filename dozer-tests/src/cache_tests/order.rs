use dozer_cache::cache::expression::{
    SortDirection::{Ascending, Descending},
    SortOption,
};
use dozer_types::types::{Record, Schema};

pub fn validate(schema: &Schema, records: &[Record], sort_options: &[SortOption]) {
    for sort_option in sort_options {
        let field_index = schema
            .fields
            .iter()
            .position(|field| field.name == sort_option.field_name)
            .unwrap();
        for (prev, current) in records.iter().zip(records.iter().skip(1)) {
            let prev_value = &prev.values[field_index];
            let current_value = &current.values[field_index];
            match sort_option.direction {
                Ascending => assert!(prev_value <= current_value),
                Descending => assert!(prev_value >= current_value),
            }
        }
    }
}
