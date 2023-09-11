use std::time::Duration;

use dozer_ingestion::{
    connectors::{CdcType, Connector, SourceSchema, TableIdentifier, TableToIngest},
    ingestion::Ingestor,
};
use dozer_types::{
    ingestion_types::IngestionMessage,
    log::{error, warn},
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema},
};
use futures::stream::{AbortHandle, Abortable};

use super::{
    data,
    records::{Operation as RecordsOperation, Records},
    CudConnectorTest, DataReadyConnectorTest, InsertOnlyConnectorTest,
};

pub async fn run_test_suite_basic_data_ready<T: DataReadyConnectorTest>() {
    let (_connector_test, connector) = T::new().await;

    // List tables.
    let tables = connector.list_tables().await.unwrap();
    connector.validate_tables(&tables).await.unwrap();

    // List columns.
    let tables = connector.list_columns(tables).await.unwrap();

    // Get schemas.
    let schemas = connector.get_schemas(&tables).await.unwrap();
    let schemas = schemas
        .into_iter()
        .map(|schema| schema.expect("Failed to get schema"))
        .collect::<Vec<_>>();

    // Run connector.
    let tables = tables
        .into_iter()
        .map(TableToIngest::from_scratch)
        .collect();
    let (ingestor, mut iterator) = Ingestor::initialize_channel(Default::default());
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    tokio::spawn(async move {
        if let Ok(Err(e)) =
            Abortable::new(connector.start(&ingestor, tables), abort_registration).await
        {
            error!("Connector `start` returned error: {e}");
        }
    });

    // Loop over messages until timeout.
    let mut last_identifier = None;
    let mut num_operations = 0;
    while let Some(message) = iterator.next_timeout(Duration::from_secs(1)) {
        // Check message identifier.
        if let IngestionMessage::OperationEvent {
            table_index,
            op,
            id,
        } = &message
        {
            if let Some(id) = id {
                if let Some(last_id) = &last_identifier {
                    assert!(id > last_id);
                }
            }
            last_identifier = *id;

            num_operations += 1;
            // Check record schema consistency.
            match op {
                Operation::Insert { new } => {
                    assert_record_matches_source_schema(new, &schemas[*table_index], true);
                }
                Operation::Update { old, new } => {
                    assert_record_matches_source_schema(old, &schemas[*table_index], false);
                    assert_record_matches_source_schema(new, &schemas[*table_index], true);
                }
                Operation::Delete { old } => {
                    assert_record_matches_source_schema(old, &schemas[*table_index], false);
                }
            }
        }
    }

    // There should be at least one message.
    assert!(num_operations > 0);
    abort_handle.abort()
}

pub async fn run_test_suite_basic_insert_only<T: InsertOnlyConnectorTest>() {
    let table_name = "test_table".to_string();
    for data_fn in [
        data::records_with_primary_key,
        data::records_without_primary_key,
    ] {
        // Load test data.
        let ((fields, primary_index), records) = data_fn();

        // Create connector.
        let schema_name = None;
        let Some((_connector_test, connector, (actual_fields, actual_primary_index))) = T::new(
            schema_name.clone(),
            table_name.clone(),
            (fields.clone(), primary_index.clone()),
            records.clone(),
        )
        .await
        else {
            warn!("Connector does not support schema name {schema_name:?} or primary index {primary_index:?}.");
            continue;
        };
        for field in &fields {
            if !actual_fields
                .iter()
                .any(|actual_field| actual_field == field)
            {
                warn!("Field {:?} is not supported by the connector.", field)
            }
        }

        // Validate connection.
        connector.validate_connection().await.unwrap();

        // Validate tables.
        connector
            .validate_tables(&[TableIdentifier::new(
                schema_name.clone(),
                table_name.clone(),
            )])
            .await
            .unwrap();

        // List columns.
        let tables = connector
            .list_columns(vec![TableIdentifier::new(schema_name, table_name.clone())])
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, table_name);
        assert_eq!(
            tables[0].column_names,
            actual_fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>()
        );

        // Validate schemas.
        let schemas = connector.get_schemas(&tables).await.unwrap();
        assert_eq!(schemas.len(), 1);
        let actual_schema = &schemas[0].as_ref().unwrap().schema;
        assert_eq!(actual_schema.fields, actual_fields);
        assert_eq!(actual_schema.primary_index, actual_primary_index);

        // Run the connector and check data is ingested.
        let tables = tables
            .into_iter()
            .map(TableToIngest::from_scratch)
            .collect();
        let (ingestor, mut iterator) = Ingestor::initialize_channel(Default::default());
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        tokio::spawn(async move {
            if let Ok(Err(e)) =
                Abortable::new(connector.start(&ingestor, tables), abort_registration).await
            {
                error!("Connector `start` returned error: {e}")
            }
        });

        let mut record_iter = records.iter();

        let mut last_identifier = None;
        while let Some(message) = iterator.next_timeout(Duration::from_secs(1)) {
            // Filter out non-operation events.
            let IngestionMessage::OperationEvent {
                op: operation, id, ..
            } = message
            else {
                continue;
            };

            // Identifier must be increasing.
            if let Some(identifier) = last_identifier {
                if let Some(id) = &id {
                    assert!(id > &identifier);
                }
                last_identifier = id;
            }

            // Operation must be insert.
            let Operation::Insert { new: actual_record } = operation else {
                panic!("Expected an insert event, but got {:?}", operation);
            };

            // Record must match schema.
            assert_record_matches_schema(&actual_record, actual_schema, false);

            // Record must match expected record.
            assert_records_match(
                &actual_record.values,
                &actual_fields,
                &actual_primary_index,
                record_iter
                    .next()
                    .expect("Connector sent more records than expected"),
                &fields,
                false,
            );
        }

        assert!(
            record_iter.next().is_none(),
            "Connector sent less records than expected."
        );
        abort_handle.abort();
    }
}

pub async fn run_test_suite_basic_cud<T: CudConnectorTest>() {
    // Load test data.
    let ((fields, primary_index), operations) = data::cud_operations();

    // Create connector.
    let schema_name = None;
    let table_name = "test_table".to_string();
    let (connector_test, connector, (_, actual_primary_index)) = T::new(
        schema_name.clone(),
        table_name.clone(),
        (fields, primary_index),
        vec![],
    )
    .await
    .unwrap();

    // Get schema.
    let tables = connector
        .list_columns(vec![TableIdentifier::new(schema_name, table_name)])
        .await
        .unwrap();
    let mut schemas = connector.get_schemas(&tables).await.unwrap();
    let actual_schema = schemas.remove(0).unwrap().schema;

    // Feed data to connector.
    connector_test.start_cud(operations.clone()).await;

    // Run the connector.
    let (ingestor, mut iterator) = Ingestor::initialize_channel(Default::default());
    tokio::spawn(async move {
        if let Err(e) = connector.start(&ingestor, vec![]).await {
            error!("Connector `start` returned error: {e}")
        }
    });

    // Check data schema consistency.
    let mut last_identifier = None;
    let mut records = Records::new(actual_primary_index.clone());
    while let Some(message) = iterator.next_timeout(Duration::from_secs(1)) {
        // Filter out non-operation events.
        let IngestionMessage::OperationEvent {
            op: operation, id, ..
        } = message
        else {
            continue;
        };

        // Identifier must be increasing.
        if let Some(identifier) = last_identifier {
            if let Some(id) = &id {
                assert!(id > &identifier);
            }
            last_identifier = id;
        }

        // Record must match schema.
        match operation {
            Operation::Insert { new } => {
                assert_record_matches_schema(&new, &actual_schema, false);
                records.append_operation(RecordsOperation::Insert { new: new.values });
            }
            Operation::Update { old, new } => {
                assert_record_matches_schema(&old, &actual_schema, false);
                assert_record_matches_schema(&new, &actual_schema, false);
                records.append_operation(RecordsOperation::Update {
                    old: old.values,
                    new: new.values,
                });
            }
            Operation::Delete { old } => {
                assert_record_matches_schema(&old, &actual_schema, false);
                records.append_operation(RecordsOperation::Delete { old: old.values });
            }
        }
    }

    // We can't check operation exact match because the connector may have batched some of them,
    // so we check that the final state is the same.
    let mut expected_records = Records::new(actual_primary_index);
    for operation in operations {
        expected_records.append_operation(operation);
    }
    assert_eq!(records, expected_records);
}

fn assert_record_matches_schema(record: &Record, schema: &Schema, only_match_pk: bool) {
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
            FieldType::U128 => {
                assert!(value.as_u128().is_some())
            }
            FieldType::Int => {
                assert!(value.as_int().is_some())
            }
            FieldType::I128 => {
                assert!(value.as_i128().is_some())
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
            FieldType::Json => assert!(value.as_json().is_some()),
            FieldType::Point => assert!(value.as_point().is_some()),
            FieldType::Duration => assert!(value.as_duration().is_some()),
        }
    }
}

fn assert_record_matches_source_schema(record: &Record, schema: &SourceSchema, full_match: bool) {
    let only_match_pk = !full_match && schema.cdc_type != CdcType::FullChanges;
    assert_record_matches_schema(record, &schema.schema, only_match_pk);
}

fn assert_records_match(
    partial_record: &[Field],
    partial_fields: &[FieldDefinition],
    partial_primary_index: &[usize],
    record: &[Field],
    fields: &[FieldDefinition],
    only_match_pk: bool,
) {
    let partial_index_to_index = partial_fields
        .iter()
        .map(|field| fields.iter().position(|f| f.name == field.name).unwrap())
        .collect::<Vec<_>>();

    for (partial_index, partial_value) in partial_record.iter().enumerate() {
        // If `only_match_pk` is true, we only check primary key fields.
        if only_match_pk && !partial_primary_index.iter().any(|i| i == &partial_index) {
            continue;
        }
        assert_eq!(
            partial_value,
            &record[partial_index_to_index[partial_index]]
        );
    }
}
