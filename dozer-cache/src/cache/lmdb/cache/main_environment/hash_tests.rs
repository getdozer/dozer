use dozer_types::types::{Record, Schema};

use super::RwMainEnvironment;

#[test]
fn test_hash_insert_delete_insert() {
    let schema = Schema::empty();
    let mut env = RwMainEnvironment::new(
        Some(&(schema, vec![])),
        &Default::default(),
        Default::default(),
    )
    .unwrap();

    let record = Record::new(None, vec![]);

    env.insert(&record).unwrap();
    env.delete(&record).unwrap();
    env.insert(&record).unwrap();
}
