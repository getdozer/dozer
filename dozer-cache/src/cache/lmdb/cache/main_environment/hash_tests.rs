use dozer_types::types::{Record, Schema};

use crate::cache::UpsertResult;

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

    let UpsertResult::Inserted { meta } = env.insert(&record).unwrap() else {
        panic!("Expected UpsertResult::Inserted");
    };
    let deleted_meta = env.delete(&record).unwrap().unwrap();
    assert_eq!(meta, deleted_meta);
    let UpsertResult::Inserted { meta } = env.insert(&record).unwrap() else {
        panic!("Expected UpsertResult::Inserted");
    };
    assert_eq!(meta.id, deleted_meta.id);
    assert_eq!(meta.version, deleted_meta.version + 1);
}
