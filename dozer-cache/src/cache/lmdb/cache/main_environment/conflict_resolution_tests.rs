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
        insert_resolution: OnInsertResolutionTypes::from(conflict_resolution.on_insert),
        delete_resolution: OnDeleteResolutionTypes::from(conflict_resolution.on_delete),
        update_resolution: OnUpdateResolutionTypes::from(conflict_resolution.on_update),
    };
    let main_env =
        RwMainEnvironment::new(Some(&schema), &Default::default(), write_options).unwrap();
    (main_env, schema.0)
}

#[test]
fn ignore_insert_error_when_type_nothing() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::Nothing as i32,
        on_update: OnUpdateResolutionTypes::default() as i32,
        on_delete: OnDeleteResolutionTypes::default() as i32,
    });

    let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
    let record = Record {
        schema_id: schema.identifier,
        values: initial_values.clone(),
    };
    env.insert(&record).unwrap();
    env.commit().unwrap();

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
        on_insert: OnInsertResolutionTypes::Update as i32,
        on_update: OnUpdateResolutionTypes::default() as i32,
        on_delete: OnDeleteResolutionTypes::default() as i32,
    });

    let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
    let record = Record {
        schema_id: schema.identifier,
        values: initial_values.clone(),
    };
    env.insert(&record).unwrap();
    env.commit().unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    assert_eq!(initial_values, record.record.values);
    assert_eq!(1, record.version);

    let second_insert_values = vec![
        Field::Int(1),
        Field::String("Second insert name".to_string()),
    ];
    let second_record = Record {
        schema_id: schema.identifier,
        values: second_insert_values.clone(),
    };

    env.insert(&second_record).unwrap();
    env.commit().unwrap();

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
        on_insert: OnInsertResolutionTypes::Panic as i32,
        on_update: OnUpdateResolutionTypes::default() as i32,
        on_delete: OnDeleteResolutionTypes::default() as i32,
    });

    let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
    let record = Record {
        schema_id: schema.identifier,
        values: initial_values.clone(),
    };
    env.insert(&record).unwrap();
    env.commit().unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    assert_eq!(initial_values, record.record.values);
    assert_eq!(1, record.version);

    // Try insert same data again
    let result = env.insert(&record.record);
    assert!(matches!(result, Err(CacheError::PrimaryKeyExists)));
}

#[test]
fn ignore_update_error_when_type_nothing() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::default() as i32,
        on_update: OnUpdateResolutionTypes::Nothing as i32,
        on_delete: OnDeleteResolutionTypes::default() as i32,
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let update_values = vec![
        Field::Int(1),
        Field::String("Film name updated".to_string()),
    ];

    let update_record = Record {
        schema_id: schema.identifier,
        values: update_values,
    };
    env.update(
        &index::get_primary_key(&schema.primary_index, &initial_values),
        &update_record,
    )
    .unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key);

    assert!(matches!(record, Err(CacheError::PrimaryKeyNotFound)));
}

#[test]
fn update_after_update_error_when_type_upsert() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::default() as i32,
        on_update: OnUpdateResolutionTypes::Upsert as i32,
        on_delete: OnDeleteResolutionTypes::default() as i32,
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let update_values = vec![
        Field::Int(1),
        Field::String("Film name updated".to_string()),
    ];

    let update_record = Record {
        schema_id: schema.identifier,
        values: update_values.clone(),
    };
    env.update(
        &index::get_primary_key(&schema.primary_index, &initial_values),
        &update_record,
    )
    .unwrap();
    env.commit().unwrap();

    let key = index::get_primary_key(&schema.primary_index, &initial_values);
    let record = env.get(&key).unwrap();

    assert_eq!(update_values, record.record.values);
    assert_eq!(1, record.version);
}

#[test]
fn return_update_error_when_type_panic() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::default() as i32,
        on_update: OnUpdateResolutionTypes::Panic as i32,
        on_delete: OnInsertResolutionTypes::default() as i32,
    });

    let initial_values = vec![Field::Int(1), Field::Null];
    let update_values = vec![
        Field::Int(1),
        Field::String("Film name updated".to_string()),
    ];

    let update_record = Record {
        schema_id: schema.identifier,
        values: update_values,
    };

    let result = env.update(
        &index::get_primary_key(&schema.primary_index, &initial_values),
        &update_record,
    );

    assert!(matches!(result, Err(CacheError::PrimaryKeyNotFound)));
}

#[test]
fn ignore_delete_error_when_type_nothing() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::default() as i32,
        on_update: OnUpdateResolutionTypes::default() as i32,
        on_delete: OnUpdateResolutionTypes::Nothing as i32,
    });

    let initial_values = vec![Field::Int(1), Field::Null];

    // Check is cache empty
    let current_count = env.count().unwrap();
    assert_eq!(current_count, 0_usize);

    // Trying delete not existing record should be ignored
    let result = env.delete(&index::get_primary_key(
        &schema.primary_index,
        &initial_values,
    ));
    assert!(result.is_ok());
}

#[test]
fn return_delete_error_when_type_panic() {
    let (mut env, schema) = init_env(ConflictResolution {
        on_insert: OnInsertResolutionTypes::default() as i32,
        on_update: OnUpdateResolutionTypes::default() as i32,
        on_delete: OnDeleteResolutionTypes::Panic as i32,
    });

    let initial_values = vec![Field::Int(1), Field::Null];

    let result = env.delete(&index::get_primary_key(
        &schema.primary_index,
        &initial_values,
    ));
    assert!(matches!(result, Err(CacheError::PrimaryKeyNotFound)));
}
