use crate::cache::index;
use crate::cache::lmdb::cache::{CacheWriteOptions, MainEnvironment};
use crate::cache::test_utils::schema_multi_indices;
use crate::errors::CacheError;
use dozer_types::models::api_endpoint::{
    ConflictResolution, OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
};
use dozer_types::types::{Field, Record, Schema};

use super::RwMainEnvironment;

fn init_env(conflict_resolution: ConflictResolution) -> (RwMainEnvironment, Schema) {
    let schema = schema_multi_indices();
    let write_options = CacheWriteOptions {
        insert_resolution: conflict_resolution.on_insert,
        delete_resolution: conflict_resolution.on_delete,
        update_resolution: conflict_resolution.on_update,
        ..Default::default()
    };
    let main_env =
        RwMainEnvironment::new(Some(&schema), None, &Default::default(), write_options).unwrap();
    (main_env, schema.0)
}

#[test]
fn ignore_insert_error_when_type_nothing() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::Nothing,
        on_update: Default::default(),
        on_delete: Default::default(),
    });

    let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
    let record = Record {
        values: initial_values.clone(),
        lifetime: None,
    };
    env.insert(&record).unwrap();
    env.commit(&Default::default()).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    assert_eq!(initial_values, record.record.values);
    assert_eq!(1, record.version);

    env.insert(&record.record).unwrap();

    let record = env.get(&key).unwrap();

    // version should remain unchanged, because insert should be ignored
    assert_eq!(initial_values, record.record.values);
    assert_eq!(1, record.version);
}

#[test]
fn update_after_insert_error_when_type_update() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::Update,
        on_update: Default::default(),
        on_delete: Default::default(),
    });

    let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
    let record = Record {
        values: initial_values.clone(),
        lifetime: None,
    };
    env.insert(&record).unwrap();
    env.commit(&Default::default()).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    assert_eq!(initial_values, record.record.values);
    assert_eq!(1, record.version);

    let second_insert_values = vec![
        Field::Int(1),
        Field::String("Second insert name".to_string()),
    ];
    let second_record = Record {
        values: second_insert_values.clone(),
        lifetime: None,
    };

    env.insert(&second_record).unwrap();
    env.commit(&Default::default()).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    // version should increase, because record should be updated
    assert_eq!(second_insert_values, record.record.values);
    assert_eq!(2, record.version);

    // Check cache size. It should have only one record
    let current_count = env.count().unwrap();
    assert_eq!(current_count, 1_usize);
}

#[test]
fn return_insert_error_when_type_panic() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::Panic,
        on_update: Default::default(),
        on_delete: Default::default(),
    });

    let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
    let record = Record {
        values: initial_values.clone(),
        lifetime: None,
    };
    env.insert(&record).unwrap();
    env.commit(&Default::default()).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    assert_eq!(initial_values, record.record.values);
    assert_eq!(1, record.version);

    // Try insert same data again
    let result = env.insert(&record.record);
    assert!(matches!(result, Err(CacheError::PrimaryKeyExists { .. })));
}

#[test]
fn ignore_update_error_when_type_nothing() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: Default::default(),
        on_update: OnUpdateResolutionTypes::Nothing,
        on_delete: Default::default(),
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let update_values = vec![
        Field::Int(1),
        Field::String("Film name updated".to_string()),
    ];

    let initial_record = Record {
        values: initial_values.clone(),
        lifetime: None,
    };
    let update_record = Record {
        values: update_values,
        lifetime: None,
    };
    env.update(&initial_record, &update_record).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key);

    assert!(matches!(record, Err(CacheError::PrimaryKeyNotFound)));
}

#[test]
fn update_after_update_error_when_type_upsert() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: Default::default(),
        on_update: OnUpdateResolutionTypes::Upsert,
        on_delete: Default::default(),
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let update_values = vec![
        Field::Int(1),
        Field::String("Film name updated".to_string()),
    ];

    let initial_record = Record {
        values: initial_values.clone(),
        lifetime: None,
    };
    let update_record = Record {
        values: update_values.clone(),
        lifetime: None,
    };
    env.update(&initial_record, &update_record).unwrap();
    env.commit(&Default::default()).unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    assert_eq!(update_values, record.record.values);
    assert_eq!(1, record.version);
}

#[test]
fn return_update_error_when_type_panic() {
    let (mut env, _) = init_env(ConflictResolution {
        on_insert: Default::default(),
        on_update: OnUpdateResolutionTypes::Panic,
        on_delete: Default::default(),
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let update_values = vec![
        Field::Int(1),
        Field::String("Film name updated".to_string()),
    ];

    let initial_record = Record {
        values: initial_values,
        lifetime: None,
    };
    let update_record = Record {
        values: update_values,
        lifetime: None,
    };

    let result = env.update(&initial_record, &update_record);

    assert!(matches!(result, Err(CacheError::PrimaryKeyNotFound)));
}

#[test]
fn ignore_delete_error_when_type_nothing() {
    let (mut env, _) = init_env(ConflictResolution {
        on_insert: Default::default(),
        on_update: Default::default(),
        on_delete: OnDeleteResolutionTypes::Nothing,
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let initial_record = Record {
        values: initial_values,
        lifetime: None,
    };

    // Check is cache empty
    let current_count = env.count().unwrap();
    assert_eq!(current_count, 0_usize);

    // Trying delete not existing record should be ignored
    let result = env.delete(&initial_record);
    assert!(result.is_ok());
}

#[test]
fn return_delete_error_when_type_panic() {
    let (mut env, _) = init_env(ConflictResolution {
        on_insert: Default::default(),
        on_update: Default::default(),
        on_delete: OnDeleteResolutionTypes::Panic,
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let initial_record = Record {
        values: initial_values,
        lifetime: None,
    };

    let result = env.delete(&initial_record);
    assert!(matches!(result, Err(CacheError::PrimaryKeyNotFound)));
}
