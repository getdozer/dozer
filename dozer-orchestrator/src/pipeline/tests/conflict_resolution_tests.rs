mod tests {
    use crate::pipeline::LogSink;
    use crate::test_utils;
    use dozer_cache::cache::expression::QueryExpression;
    use dozer_cache::cache::{index, RoCache};
    use dozer_cache::errors::CacheError;
    use dozer_core::errors::{ExecutionError, SinkError};
    use dozer_core::node::Sink;
    use dozer_core::DEFAULT_PORT_HANDLE;
    use dozer_types::models::api_endpoint::{
        ConflictResolution, OnDeleteResolutionTypes, OnInsertResolutionTypes,
        OnUpdateResolutionTypes,
    };
    use dozer_types::types::{Field, IndexDefinition, Operation, Record, Schema, SchemaIdentifier};

    fn init_cache_and_sink(
        conflict_resolution: Option<ConflictResolution>,
    ) -> (Box<dyn RoCache>, LogSink, Schema) {
        let schema = test_utils::get_schema();
        let secondary_indexes: Vec<IndexDefinition> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![idx]))
            .collect();

        let (cache_manager, sink) =
            test_utils::init_sink(schema.clone(), secondary_indexes, conflict_resolution);

        let cache = cache_manager.open_ro_cache("films").unwrap().unwrap();

        (cache, sink, schema)
    }

    #[test]
    #[ignore]
    fn ignore_insert_error_when_type_nothing() {
        let (cache, mut sink, schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::Nothing as i32,
            on_update: OnUpdateResolutionTypes::default() as i32,
            on_delete: OnDeleteResolutionTypes::default() as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
        let insert_operation = Operation::Insert {
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
                version: None,
            },
        };
        sink.process(DEFAULT_PORT_HANDLE, insert_operation.clone())
            .unwrap();
        sink.commit().unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap().record;

        assert_eq!(initial_values, record.values);
        assert_eq!(Some(1), record.version);

        sink.process(DEFAULT_PORT_HANDLE, insert_operation).unwrap();
        sink.commit().unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap().record;

        // version should remain unchanged, because insert should be ignored
        assert_eq!(initial_values, record.values);
        assert_eq!(Some(1), record.version);
    }

    #[test]
    #[ignore]
    fn update_after_insert_error_when_type_update() {
        let (cache, mut sink, schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::Update as i32,
            on_update: OnUpdateResolutionTypes::default() as i32,
            on_delete: OnDeleteResolutionTypes::default() as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
        let insert_operation = Operation::Insert {
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
                version: None,
            },
        };
        sink.process(DEFAULT_PORT_HANDLE, insert_operation).unwrap();
        sink.commit().unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap().record;

        assert_eq!(initial_values, record.values);
        assert_eq!(Some(1), record.version);

        let second_insert_values = vec![
            Field::Int(1),
            Field::String("Second insert name".to_string()),
        ];
        let second_insert_operation = Operation::Insert {
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: second_insert_values.clone(),
                version: None,
            },
        };

        sink.process(DEFAULT_PORT_HANDLE, second_insert_operation)
            .unwrap();
        sink.commit().unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap().record;

        // version should remain unchanged, because insert should be ignored
        assert_eq!(second_insert_values, record.values);
        assert_eq!(Some(2), record.version);

        // Check cache size. It should have only one record
        let current_count = cache.count(&QueryExpression::default()).unwrap();
        assert_eq!(current_count, 1_usize);
    }

    #[test]
    #[ignore]
    fn return_insert_error_when_type_panic() {
        let (cache, mut sink, schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::Panic as i32,
            on_update: OnUpdateResolutionTypes::default() as i32,
            on_delete: OnDeleteResolutionTypes::default() as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::String("Film name old".to_string())];
        let insert_operation = Operation::Insert {
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
                version: None,
            },
        };
        sink.process(DEFAULT_PORT_HANDLE, insert_operation.clone())
            .unwrap();
        sink.commit().unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap().record;

        assert_eq!(initial_values, record.values);
        assert_eq!(Some(1), record.version);

        // Try insert same data again
        let result = sink.process(DEFAULT_PORT_HANDLE, insert_operation);
        assert!(matches!(
            result,
            Err(ExecutionError::SinkError(SinkError::CacheInsertFailed(
                _,
                _
            )))
        ));
    }

    #[test]
    #[ignore]
    fn ignore_update_error_when_type_nothing() {
        let (cache, mut sink, schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::default() as i32,
            on_update: OnUpdateResolutionTypes::Nothing as i32,
            on_delete: OnDeleteResolutionTypes::default() as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::Null];
        let update_values = vec![
            Field::Int(1),
            Field::String("Film name updated".to_string()),
        ];

        let update_operation = Operation::Update {
            old: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
                version: None,
            },
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: update_values,
                version: None,
            },
        };
        sink.process(DEFAULT_PORT_HANDLE, update_operation).unwrap();
        sink.commit().unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key);

        assert!(matches!(record, Err(CacheError::PrimaryKeyNotFound)));
    }

    #[test]
    #[ignore]
    fn update_after_update_error_when_type_update() {
        let (cache, mut sink, schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::default() as i32,
            on_update: OnUpdateResolutionTypes::Upsert as i32,
            on_delete: OnDeleteResolutionTypes::default() as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::Null];
        let update_values = vec![
            Field::Int(1),
            Field::String("Film name updated".to_string()),
        ];

        let update_operation = Operation::Update {
            old: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values.clone(),
                version: None,
            },
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: update_values.clone(),
                version: None,
            },
        };
        sink.process(DEFAULT_PORT_HANDLE, update_operation).unwrap();
        sink.commit().unwrap();

        let key = index::get_primary_key(&schema.primary_index, &initial_values);
        let record = cache.get(&key).unwrap().record;

        assert_eq!(update_values, record.values);
        assert_eq!(Some(1), record.version);
    }

    #[test]
    #[ignore]
    fn return_update_error_when_type_panic() {
        let (_cache, mut sink, _schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::default() as i32,
            on_update: OnUpdateResolutionTypes::Panic as i32,
            on_delete: OnInsertResolutionTypes::default() as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::Null];
        let update_values = vec![
            Field::Int(1),
            Field::String("Film name updated".to_string()),
        ];

        let update_operation = Operation::Update {
            old: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values,
                version: None,
            },
            new: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: update_values,
                version: None,
            },
        };

        // Try insert same data again
        let result = sink.process(DEFAULT_PORT_HANDLE, update_operation);
        assert!(matches!(
            result,
            Err(ExecutionError::SinkError(SinkError::CacheUpdateFailed(
                _,
                _
            )))
        ));
    }

    #[test]
    #[ignore]
    fn ignore_delete_error_when_type_nothing() {
        let (cache, mut sink, _schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::default() as i32,
            on_update: OnUpdateResolutionTypes::default() as i32,
            on_delete: OnUpdateResolutionTypes::Nothing as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::Null];

        let delete_operation = Operation::Delete {
            old: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values,
                version: None,
            },
        };

        // Check is cache empty
        let current_count = cache.count(&QueryExpression::default()).unwrap();
        assert_eq!(current_count, 0_usize);

        // Trying delete not existing record should be ignored
        let result = sink.process(DEFAULT_PORT_HANDLE, delete_operation);
        assert!(result.is_ok());
    }
    #[test]
    #[ignore]
    fn return_delete_error_when_type_panic() {
        let (_cache, mut sink, _schema) = init_cache_and_sink(Some(ConflictResolution {
            on_insert: OnInsertResolutionTypes::default() as i32,
            on_update: OnUpdateResolutionTypes::default() as i32,
            on_delete: OnDeleteResolutionTypes::Panic as i32,
        }));

        let initial_values = vec![Field::Int(1), Field::Null];

        let update_operation = Operation::Delete {
            old: Record {
                schema_id: Option::from(SchemaIdentifier { id: 1, version: 1 }),
                values: initial_values,
                version: None,
            },
        };

        // Try insert same data again
        let result = sink.process(DEFAULT_PORT_HANDLE, update_operation);
        assert!(matches!(
            result,
            Err(ExecutionError::SinkError(SinkError::CacheDeleteFailed(
                _,
                _
            )))
        ));
    }
}
