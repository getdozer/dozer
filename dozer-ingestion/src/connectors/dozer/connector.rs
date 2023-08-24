use std::collections::HashMap;

use dozer_log::{
    reader::{LogReaderBuilder, LogReaderOptions},
    replication::LogOperation,
};
use dozer_types::{
    errors::types::DeserializationError,
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient,
        DescribeApplicationRequest, DescribeApplicationResponse,
    },
    ingestion_types::{
        default_log_options, IngestionMessage, NestedDozerConfig, NestedDozerLogOptions,
    },
    serde_json,
    types::{Operation, Record, Schema},
};
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinSet,
};
use tonic::{async_trait, transport::Channel};

use crate::{
    connectors::{
        warn_dropped_primary_index, CdcType, Connector, SourceSchema, SourceSchemaResult,
        TableIdentifier, TableInfo,
    },
    errors::{ConnectorError, NestedDozerConnectorError},
    ingestion::Ingestor,
};

#[derive(Debug)]
pub struct NestedDozerConnector {
    config: NestedDozerConfig,
}

#[async_trait]
impl Connector for NestedDozerConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        let _ = self.get_client().await?;

        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        let mut tables = vec![];
        let response = self.describe_application().await?;
        for (endpoint, _) in response.endpoints {
            tables.push(TableIdentifier::new(None, endpoint));
        }

        Ok(tables)
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        self.validate_connection().await?;

        for table in tables {
            self.get_reader_builder(table.name.clone()).await?;
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        _tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let mut tables = vec![];
        let response = self.describe_application().await?;
        for (endpoint, build) in response.endpoints {
            let schema: SourceSchema = serde_json::from_str(&build.schema_string).map_err(|e| {
                ConnectorError::TypeError(
                    dozer_types::errors::types::TypeError::DeserializationError(
                        DeserializationError::Json(e),
                    ),
                )
            })?;
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
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        let mut schemas = vec![];
        for table_info in table_infos {
            let log_reader = self.get_reader_builder(table_info.name.clone()).await;

            schemas.push(log_reader.and_then(|log_reader| {
                let source_primary_index_len = log_reader.schema.schema.primary_index.len();
                let source_schema = log_reader.schema.schema.clone();
                let schema_mapper = SchemaMapper::new(source_schema, &table_info.column_names)?;
                let mut schema = schema_mapper.map()?;
                if schema.primary_index.len() < source_primary_index_len {
                    schema.primary_index.clear();
                    warn_dropped_primary_index(&table_info.name);
                }

                Ok(SourceSchema::new(schema, CdcType::FullChanges))
            }));
        }

        Ok(schemas)
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        let mut joinset = JoinSet::new();
        let (sender, mut receiver) = channel(100);

        for (table_index, table) in tables.into_iter().enumerate() {
            let builder = self.get_reader_builder(table.name.clone()).await?;
            joinset.spawn(read_table(table_index, table, builder, sender.clone()));
        }

        let ingestor = ingestor.clone();
        joinset.spawn(async move {
            let mut seq_no = 0;
            while let Some((table_idx, op)) = receiver.recv().await {
                ingestor
                    .handle_message(IngestionMessage::new_op(0, seq_no, table_idx, op))
                    .map_err(ConnectorError::IngestorError)?;
                seq_no += 1;
            }
            Ok(())
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
    async fn get_client(&self) -> Result<InternalPipelineServiceClient<Channel>, ConnectorError> {
        let app_server_addr = self.get_server_addr()?;
        let client = InternalPipelineServiceClient::connect(app_server_addr)
            .await
            .map_err(|e| {
                ConnectorError::NestedDozerConnectorError(
                    NestedDozerConnectorError::ConnectionError(e),
                )
            })?;
        Ok(client)
    }

    async fn describe_application(&self) -> Result<DescribeApplicationResponse, ConnectorError> {
        let mut client = self.get_client().await?;

        let response = client
            .describe_application(DescribeApplicationRequest {})
            .await
            .map_err(|e| {
                ConnectorError::NestedDozerConnectorError(
                    NestedDozerConnectorError::DescribeEndpointsError(e),
                )
            })?;

        Ok(response.into_inner())
    }

    fn get_server_addr(&self) -> Result<String, ConnectorError> {
        let config = self
            .config
            .grpc
            .as_ref()
            .ok_or(ConnectorError::MissingConfiguration("grpc".to_owned()))?;
        Ok(format!("http://{}:{}", &config.host, &config.port))
    }

    fn get_log_options(endpoint: String, value: NestedDozerLogOptions) -> LogReaderOptions {
        LogReaderOptions {
            endpoint,
            batch_size: value.batch_size,
            timeout_in_millis: value.timeout_in_millis,
            buffer_size: value.buffer_size,
        }
    }

    async fn get_reader_builder(
        &self,
        endpoint: String,
    ) -> Result<LogReaderBuilder, ConnectorError> {
        let app_server_addr = self.get_server_addr()?;

        let log_options = match self.config.log_options.as_ref() {
            Some(opts) => opts.clone(),
            None => default_log_options(),
        };
        let log_options = Self::get_log_options(endpoint, log_options);
        let log_reader_builder = LogReaderBuilder::new(app_server_addr, log_options)
            .await
            .map_err(NestedDozerConnectorError::ReaderBuilderError)?;
        Ok(log_reader_builder)
    }
}

async fn read_table(
    table_idx: usize,
    table_info: TableInfo,
    reader_builder: LogReaderBuilder,
    sender: Sender<(usize, Operation)>,
) -> Result<(), ConnectorError> {
    let mut reader = reader_builder.build(0, None);
    let schema = reader.schema.schema.clone();
    let map = SchemaMapper::new(schema, &table_info.column_names)?;
    loop {
        let (op, _) = reader.next_op().await.map_err(|e| {
            ConnectorError::NestedDozerConnectorError(NestedDozerConnectorError::ReaderError(e))
        })?;
        let op = match op {
            LogOperation::Op { op } => op,
            LogOperation::Commit { .. } | LogOperation::SnapshottingDone { .. } => continue,
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
        };

        // If the other side of the channel is dropped, they are handling the error
        let _ = sender.send((table_idx, op)).await;
    }
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
    ) -> Result<SchemaMapper, ConnectorError> {
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
                return Err(ConnectorError::NestedDozerConnectorError(
                    NestedDozerConnectorError::ColumnNotFound(column.to_owned()),
                ));
            }
        }

        Ok(Self {
            source_schema,
            fields: our_fields,
            primary_index,
        })
    }

    fn map(self) -> Result<Schema, ConnectorError> {
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
    ) -> Result<Schema, ConnectorError> {
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
