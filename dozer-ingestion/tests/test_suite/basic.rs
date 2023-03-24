use std::{collections::HashMap, time::Duration};

use dozer_ingestion::{
    connectors::{CdcType, Connector, SourceSchema, TableIdentifier},
    ingestion::Ingestor,
};
use dozer_types::{
    ingestion_types::IngestionMessageKind,
    log::warn,
    types::{Field, FieldType, Operation, Record, Schema, SchemaIdentifier},
};

use super::{data, DataReadyConnectorTest, InsertOnlyConnectorTest};

pub fn run_test_suite_basic_data_ready<T: DataReadyConnectorTest>() {
    let connector_test = T::new();

    // List tables.
    let tables = connector_test.connector().list_tables().unwrap();
    connector_test.connector().validate_tables(&tables).unwrap();

    // List columns.
    let tables = connector_test.connector().list_columns(tables).unwrap();

    // Get schemas.
    let schemas = connector_test.connector().get_schemas(&tables).unwrap();
    let schemas = schemas
        .into_iter()
        .map(|schema| {
            let schema = schema.expect("Failed to get schema");
            let identifier = schema.schema.identifier.expect("Schema has no identifier");
            (identifier, schema)
        })
        .collect::<HashMap<_, _>>();

    // Run connector.
    let (ingestor, mut iterator) = Ingestor::initialize_channel(Default::default());
    std::thread::spawn(move || {
        connector_test.connector().start(&ingestor, tables).unwrap();
    });

    // Loop over messages until timeout.
    let mut last_identifier = None;
    let mut num_operations = 0;
    while let Some(message) = iterator.next_timeout(Duration::from_secs(1)) {
        // Check message identifier.
        if let Some(last_identifier) = last_identifier {
            assert!(message.identifier > last_identifier);
        }
        last_identifier = Some(message.identifier);

        if let IngestionMessageKind::OperationEvent(operation) = &message.kind {
            num_operations += 1;
            // Check record schema consistency.
            match operation {
                Operation::Insert { new } => {
                    assert_record_matches_one_schema(new, &schemas, true);
                }
                Operation::Update { old, new } => {
                    assert_record_matches_one_schema(old, &schemas, false);
                    assert_record_matches_one_schema(new, &schemas, true);
                }
                Operation::Delete { old } => {
                    assert_record_matches_one_schema(old, &schemas, false);
                }
            }
        }
    }

    // There should be at least one message.
    assert!(num_operations > 0);
}

pub fn run_test_suite_basic_insert_only<T: InsertOnlyConnectorTest>() {
    let table_name = "test_table".to_string();
    for data_fn in [
        data::records_with_primary_key,
        data::records_without_primary_key,
    ] {
        // Load test data.
        let (schema, records) = data_fn();

        // Create connector.
        let schema_name = None;
        let Some((connector_test, actual_schema)) = T::new(
            schema_name.clone(),
            table_name.clone(),
            schema.clone(),
            records.clone(),
        ) else {
            warn!("Connector does not support schema name {schema_name:?} or primary index {:?}.", schema.primary_index);
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

        // Run the connector and check data is ingested.
        let (ingestor, mut iterator) = Ingestor::initialize_channel(Default::default());
        std::thread::spawn(move || {
            connector_test
                .connector()
                .start(&ingestor, tables)
                .expect("Failed to start connector.");
        });

        let mut last_identifier = None;
        for record in &records {
            // Connector must send message.
            let message = iterator.next().unwrap();

            // Identifier must be increasing.
            if let Some(identifier) = last_identifier {
                assert!(message.identifier > identifier);
                last_identifier = Some(message.identifier);
            }

            // Message must be an insert event.
            let IngestionMessageKind::OperationEvent(Operation::Insert { new: actual_record }) = message.kind else {
                panic!("Expected an insert event, but got {:?}", message.kind);
            };

            // Record must match schema.
            assert_record_matches_schema(&actual_record, &actual_schema, false);

            // Record must match expected record.
            assert_records_match(&actual_record, &actual_schema, record, &schema, false);
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

fn assert_record_matches_one_schema(
    record: &Record,
    schemas: &HashMap<SchemaIdentifier, SourceSchema>,
    full_match: bool,
) {
    let identifier = record
        .schema_id
        .expect("Record must have a schema identifier.");
    let schema = schemas.get(&identifier).expect("Schema must exist.");

    let only_match_pk = !full_match && schema.cdc_type != CdcType::FullChanges;
    assert_record_matches_schema(record, &schema.schema, only_match_pk);
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
