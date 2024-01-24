use std::collections::HashMap;

use dozer_ingestion_connector::{
    dozer_types::{
        self,
        errors::internal::BoxedError,
        grpc_types::internal::{
            internal_pipeline_service_client::InternalPipelineServiceClient,
            DescribeApplicationResponse,
        },
        models::ingestion_types::{
            default_buffer_size, default_log_batch_size, default_timeout, IngestionMessage,
            NestedDozerConfig, NestedDozerLogOptions,
        },
        node::RestartableState,
        serde_json,
        tonic::{async_trait, transport::Channel},
        types::{FieldType, Operation, Record, Schema},
    },
    tokio::{
        sync::mpsc::{channel, Sender},
        task::JoinSet,
    },
    utils::warn_dropped_primary_index,
    CdcType, Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
};
use dozer_log::{
    reader::{LogReaderBuilder, LogReaderOptions},
    replication::LogOperation,
};

use crate::NestedDozerConnectorError;

#[derive(Debug)]
pub struct NestedDozerConnector {
    config: NestedDozerConfig,
}

#[async_trait]
impl Connector for NestedDozerConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), BoxedError> {
        let _ = self.get_client().await?;

        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, BoxedError> {
        let mut tables = vec![];
        let response = self.describe_application().await?;
        for (endpoint, _) in response.endpoints {
            tables.push(TableIdentifier::new(None, endpoint));
        }

        Ok(tables)
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        self.validate_connection().await?;

        for table in tables {
            self.get_reader_builder(table.name.clone()).await?;
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        _tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let mut tables = vec![];
        let response = self.describe_application().await?;
        for (endpoint, build) in response.endpoints {
            let schema: SourceSchema = serde_json::from_str(&build.schema_string)?;
            tables.push(TableInfo {
                schema: None,
                name: endpoint,
                column_names: schema
                    .schema
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
            });
        }
        Ok(tables)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let mut schemas = vec![];
        for table_info in table_infos {
            let log_reader = self.get_reader_builder(table_info.name.clone()).await;

            schemas.push(
                log_reader
                    .and_then(|log_reader| {
                        let source_primary_index_len = log_reader.schema.schema.primary_index.len();
                        let source_schema = log_reader.schema.schema;
                        let schema_mapper =
                            SchemaMapper::new(source_schema, &table_info.column_names)?;
                        let mut schema = schema_mapper.map()?;
                        if schema.primary_index.len() < source_primary_index_len {
                            schema.primary_index.clear();
                            warn_dropped_primary_index(&table_info.name);
                        }

                        Ok(SourceSchema::new(schema, CdcType::FullChanges))
                    })
                    .map_err(Into::into),
            );
        }

        Ok(schemas)
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: Option<RestartableState>,
    ) -> Result<(), BoxedError> {
        let mut joinset = JoinSet::new();
        let (sender, mut receiver) = channel(100);

        for (table_index, table) in tables.into_iter().enumerate() {
            let builder = self.get_reader_builder(table.name.clone()).await?;
            joinset.spawn(read_table(
                table_index,
                table,
                last_checkpoint.clone(),
                builder,
                sender.clone(),
            ));
        }

        let ingestor = ingestor.clone();
        joinset.spawn(async move {
            while let Some(message) = receiver.recv().await {
                // If the other side of the channel is dropped, return Ok.
                let _ = ingestor.handle_message(message).await;
            }
            Ok::<_, NestedDozerConnectorError>(())
        });

        while let Some(result) = joinset.join_next().await {
            // Unwrap to propagate panics inside the tasks
            // Return on first non-panic error.
            // The JoinSet will abort all other tasks on drop
            let _ = result.unwrap()?;
        }
        Ok(())
    }
}

impl NestedDozerConnector {
    pub fn new(config: NestedDozerConfig) -> Self {
        Self { config }
    }
    async fn get_client(
        &self,
    ) -> Result<InternalPipelineServiceClient<Channel>, NestedDozerConnectorError> {
        let client = InternalPipelineServiceClient::connect(self.config.url.clone())
            .await
            .map_err(|e| NestedDozerConnectorError::ConnectionError(self.config.url.clone(), e))?;
        Ok(client)
    }

    async fn describe_application(
        &self,
    ) -> Result<DescribeApplicationResponse, NestedDozerConnectorError> {
        let mut client = self.get_client().await?;

        let response = client
            .describe_application(())
            .await
            .map_err(NestedDozerConnectorError::DescribeEndpointsError)?;

        Ok(response.into_inner())
    }

    fn get_log_options(value: NestedDozerLogOptions) -> LogReaderOptions {
        LogReaderOptions {
            batch_size: value.batch_size.unwrap_or_else(default_log_batch_size),
            timeout_in_millis: value.timeout_in_millis.unwrap_or_else(default_timeout),
            buffer_size: value.buffer_size.unwrap_or_else(default_buffer_size),
        }
    }

    async fn get_reader_builder(
        &self,
        endpoint: String,
    ) -> Result<LogReaderBuilder, NestedDozerConnectorError> {
        let log_options = Self::get_log_options(self.config.log_options.clone());
        let log_reader_builder =
            LogReaderBuilder::new(self.config.url.clone(), endpoint, log_options)
                .await
                .map_err(NestedDozerConnectorError::ReaderBuilderError)?;
        Ok(log_reader_builder)
    }
}

async fn read_table(
    table_index: usize,
    table_info: TableInfo,
    last_checkpoint: Option<RestartableState>,
    reader_builder: LogReaderBuilder,
    sender: Sender<IngestionMessage>,
) -> Result<(), NestedDozerConnectorError> {
    let state = last_checkpoint
        .map(|state| decode_state(&state))
        .transpose()?;
    let starting_point = state.map(|pos| pos + 1).unwrap_or(0);
    let mut reader = reader_builder.build(starting_point);
    let schema = reader.schema.schema.clone();
    let map = SchemaMapper::new(schema, &table_info.column_names)?;
    loop {
        let op_and_pos = reader
            .read_one()
            .await
            .map_err(NestedDozerConnectorError::ReaderError)?;
        let op = match op_and_pos.op {
            LogOperation::Op { op } => op,
            LogOperation::Commit { .. }
            | LogOperation::SnapshottingStarted { .. }
            | LogOperation::SnapshottingDone { .. } => continue,
        };

        let op = match op {
            Operation::Delete { old } => Operation::Delete {
                old: map.map_record(old),
            },
            Operation::Insert { new } => Operation::Insert {
                new: map.map_record(new),
            },
            Operation::Update { old, new } => Operation::Update {
                old: map.map_record(old),
                new: map.map_record(new),
            },
            Operation::BatchInsert { new } => Operation::BatchInsert {
                new: new
                    .into_iter()
                    .map(|record| map.map_record(record))
                    .collect(),
            },
        };

        // If the other side of the channel is dropped, they are handling the error
        let _ = sender
            .send(IngestionMessage::OperationEvent {
                table_index,
                op,
                state: Some(encode_state(op_and_pos.pos)),
            })
            .await;
    }
}

fn encode_state(pos: u64) -> RestartableState {
    pos.to_be_bytes().to_vec().into()
}

fn decode_state(state: &RestartableState) -> Result<u64, NestedDozerConnectorError> {
    Ok(u64::from_be_bytes(
        state
            .0
            .as_slice()
            .try_into()
            .map_err(|_| NestedDozerConnectorError::CorruptedState)?,
    ))
}

struct SchemaMapper {
    source_schema: Schema,
    fields: Vec<usize>,
    // The primary index as indices in `fields`
    primary_index: Vec<usize>,
}

fn reorder<'a, T>(values: &'a [T], indices: &'a [usize]) -> impl Iterator<Item = T> + 'a
where
    T: Clone,
{
    indices.iter().map(|index| values[*index].clone())
}

impl SchemaMapper {
    fn new(
        source_schema: dozer_types::types::Schema,
        columns: &[String],
    ) -> Result<SchemaMapper, NestedDozerConnectorError> {
        let mut our_fields = Vec::with_capacity(columns.len());
        let upstream_fields: HashMap<String, (usize, bool)> = source_schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                (
                    field.name.clone(),
                    (i, source_schema.primary_index.contains(&i)),
                )
            })
            .collect();

        let mut primary_index = Vec::with_capacity(source_schema.primary_index.len());
        for (i, column) in columns.iter().enumerate() {
            if let Some((idx, is_primary_key)) = upstream_fields.get(column) {
                our_fields.push(*idx);
                if *is_primary_key {
                    primary_index.push(i);
                }
            } else {
                return Err(NestedDozerConnectorError::ColumnNotFound(column.to_owned()));
            }
        }

        Ok(Self {
            source_schema,
            fields: our_fields,
            primary_index,
        })
    }

    fn map(self) -> Result<Schema, NestedDozerConnectorError> {
        let field_definitions = reorder(&self.source_schema.fields, &self.fields)
            .map(|mut field| {
                field.source = Default::default();
                field
            })
            .collect();

        Ok(Schema {
            fields: field_definitions,
            primary_index: self.primary_index,
        })
    }

    fn map_record(&self, record: Record) -> Record {
        let values = record.values;
        Record::new(reorder(&values, &self.fields).collect())
    }
}

#[cfg(test)]
mod tests {
    use dozer_types::types::{Field, FieldDefinition, FieldType};

    use super::*;

    fn fields(column_names: &[&'static str]) -> Vec<FieldDefinition> {
        column_names
            .iter()
            .map(|name| FieldDefinition {
                name: (*name).to_owned(),
                typ: FieldType::Int,
                nullable: true,
                source: Default::default(),
            })
            .collect()
    }

    fn columns(column_names: &[&'static str]) -> Vec<String> {
        column_names.iter().map(|s| (*s).to_owned()).collect()
    }

    fn map(
        source_schema: Schema,
        output_fields: &[&'static str],
    ) -> Result<Schema, NestedDozerConnectorError> {
        let mapper = SchemaMapper::new(source_schema, &columns(output_fields))?;

        mapper.map()
    }

    #[test]
    fn test_map_schema_rearranges_cols() {
        assert_eq!(
            map(
                Schema {
                    fields: fields(&["0", "1", "2"]),
                    primary_index: vec![]
                },
                &["0", "2", "1"]
            )
            .unwrap()
            .fields,
            fields(&["0", "2", "1"])
        );
    }

    #[test]
    fn test_map_schema_maps_primary_key() {
        let source_schema = Schema {
            fields: fields(&["0", "1"]),
            primary_index: vec![0],
        };

        assert_eq!(
            map(source_schema, &["1", "0"]).unwrap().primary_index,
            vec![1]
        );
    }

    #[test]
    fn test_map_schema_missing_col_err() {
        let source_schema = Schema {
            fields: fields(&["0", "2"]),
            primary_index: vec![],
        };
        assert!(map(source_schema, &["0", "1"]).is_err());
    }

    #[test]
    fn test_map_record() {
        let source_schema = Schema {
            fields: fields(&["0", "1"]),
            primary_index: vec![],
        };

        let mapper = SchemaMapper::new(source_schema, &columns(&["1", "0"])).unwrap();

        assert_eq!(
            mapper.map_record(Record::new(vec![Field::UInt(0), Field::UInt(1)])),
            Record::new(vec![Field::UInt(1), Field::UInt(0)])
        )
    }
}
