use dozer_ingestion::{
    connectors::{CdcType, Connector, TableIdentifier},
    ingestion::Ingestor,
};
use dozer_types::{
    ingestion_types::IngestionMessageKind,
    log::warn,
    types::{Field, FieldType, Operation, Record, Schema},
};

use super::{data, ConnectorTest};

pub fn run_test_suite_basic<T: ConnectorTest>() {
    let table_name = "test_table".to_string();
    for data_fn in [
        data::append_only_operations_without_primary_key,
        data::append_only_operations_with_primary_key,
        data::all_kinds_of_operations,
    ] {
        // Load test data.
        let (schema, operations) = data_fn();

        // Create connector.
        let schema_name = None;
        let Some((connector_test, actual_schema)) = T::new(
            schema_name.clone(),
            table_name.clone(),
            schema.clone(),
            operations.clone(),
        ) else {
            warn!("Connector does not support schema {:?} or some operations.", schema_name);
            continue;
        };
        assert_eq!(schema.identifier, actual_schema.identifier);
        for field in &schema.fields {
            if !actual_schema
                .fields
                .iter()
                .any(|actual_field| actual_field == field)
            {
                warn!("Field {:?} is not supported by the connector.", field)
            }
        }

        // Validate connection.
        connector_test.connector().validate_connection().unwrap();

        // Validate tables.
        connector_test
            .connector()
            .validate_tables(&[TableIdentifier::new(
                schema_name.clone(),
                table_name.clone(),
            )])
            .unwrap();

        // List columns.
        let tables = connector_test
            .connector()
            .list_columns(vec![TableIdentifier::new(schema_name, table_name.clone())])
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, table_name);
        assert_eq!(
            tables[0].column_names,
            actual_schema
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>()
        );

        // Validate schemas.
        let schemas = connector_test.connector().get_schemas(&tables).unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].as_ref().unwrap().schema, actual_schema);
        let cdc_type = schemas[0].as_ref().unwrap().cdc_type;

        // Run the connector and check data is ingested.
        let (ingestor, mut iterator) = Ingestor::initialize_channel(Default::default());
        std::thread::spawn(move || {
            connector_test.start();
            connector_test
                .connector()
                .start(&ingestor, tables)
                .expect("Failed to start connector.");
        });

        let mut last_identifier = None;
        for operation in operations {
            // Connector must send message.
            let message = iterator.next().unwrap();

            // Identifier must be increasing.
            if let Some(identifier) = last_identifier {
                assert!(message.identifier > identifier);
                last_identifier = Some(message.identifier);
            }

            // Message must be an operation event.
            let IngestionMessageKind::OperationEvent(actual_operation) = message.kind else {
                panic!("Expected an operation event, but got {:?}", message.kind);
            };

            // Record in operation must match schema.
            match &actual_operation {
                Operation::Insert { new } => {
                    assert_record_matches_schema(new, &actual_schema, false)
                }
                Operation::Update { new, old } => {
                    assert_record_matches_schema(new, &actual_schema, false);
                    match cdc_type {
                        CdcType::FullChanges => {
                            assert_record_matches_schema(old, &actual_schema, false)
                        }
                        CdcType::OnlyPK => assert_record_matches_schema(old, &actual_schema, true),
                        CdcType::Nothing => {
                            panic!("Connector with CdcType::Nothing should not send update events.")
                        }
                    }
                }
                Operation::Delete { old } => match cdc_type {
                    CdcType::FullChanges => {
                        assert_record_matches_schema(old, &actual_schema, false)
                    }
                    CdcType::OnlyPK => assert_record_matches_schema(old, &actual_schema, true),
                    CdcType::Nothing => {
                        panic!("Connector with CdcType::Nothing should not send delete events.")
                    }
                },
            }

            // Operation must match expected operation.
            match (&actual_operation, &operation) {
                (Operation::Insert { new: actual_new }, Operation::Insert { new }) => {
                    assert_records_match(actual_new, &actual_schema, new, &schema, false);
                }
                (
                    Operation::Update {
                        new: actual_new,
                        old: actual_old,
                    },
                    Operation::Update { new, old },
                ) => {
                    assert_records_match(actual_new, &actual_schema, new, &schema, false);
                    match cdc_type {
                        CdcType::FullChanges => {
                            assert_records_match(actual_old, &actual_schema, old, &schema, false)
                        }
                        CdcType::OnlyPK => {
                            assert_records_match(actual_old, &actual_schema, old, &schema, true)
                        }
                        CdcType::Nothing => {
                            panic!("Connector with CdcType::Nothing should not send update events.")
                        }
                    }
                }
                (Operation::Delete { old: actual_old }, Operation::Delete { old }) => {
                    match cdc_type {
                        CdcType::FullChanges => {
                            assert_records_match(actual_old, &actual_schema, old, &schema, false)
                        }
                        CdcType::OnlyPK => {
                            assert_records_match(actual_old, &actual_schema, old, &schema, true)
                        }
                        CdcType::Nothing => {
                            panic!("Connector with CdcType::Nothing should not send delete events.")
                        }
                    }
                }
                _ => panic!("Expected {:?}, but got {:?}", operation, actual_operation),
            }
        }
    }
}

fn assert_record_matches_schema(record: &Record, schema: &Schema, only_match_pk: bool) {
    assert_eq!(record.schema_id, schema.identifier);
    assert_eq!(record.values.len(), schema.fields.len());
    for (index, (field, value)) in schema.fields.iter().zip(record.values.iter()).enumerate() {
        // If `only_match_pk` is true, we only check primary key fields.
        if only_match_pk && !schema.primary_index.iter().any(|i| i == &index) {
            continue;
        }
        if field.nullable && value == &Field::Null {
            continue;
        }
        match field.typ {
            FieldType::UInt => {
                assert!(value.as_uint().is_some())
            }
            FieldType::Int => {
                assert!(value.as_int().is_some())
            }
            FieldType::Float => {
                assert!(value.as_float().is_some())
            }
            FieldType::Boolean => assert!(value.as_boolean().is_some()),
            FieldType::String => assert!(value.as_string().is_some()),
            FieldType::Text => assert!(value.as_text().is_some()),
            FieldType::Binary => assert!(value.as_binary().is_some()),
            FieldType::Decimal => assert!(value.as_decimal().is_some()),
            FieldType::Timestamp => assert!(value.as_timestamp().is_some()),
            FieldType::Date => assert!(value.as_date().is_some()),
            FieldType::Bson => assert!(value.as_bson().is_some()),
            FieldType::Point => assert!(value.as_point().is_some()),
        }
    }
}

fn assert_records_match(
    partial_record: &Record,
    partial_schema: &Schema,
    record: &Record,
    schema: &Schema,
    only_match_pk: bool,
) {
    let partial_index_to_index = partial_schema
        .fields
        .iter()
        .map(|field| {
            schema
                .fields
                .iter()
                .position(|f| f.name == field.name)
                .unwrap()
        })
        .collect::<Vec<_>>();

    for (partial_index, partial_value) in partial_record.values.iter().enumerate() {
        // If `only_match_pk` is true, we only check primary key fields.
        if only_match_pk
            && !partial_schema
                .primary_index
                .iter()
                .any(|i| i == &partial_index)
        {
            continue;
        }
        assert_eq!(
            partial_value,
            &record.values[partial_index_to_index[partial_index]]
        );
    }
}
