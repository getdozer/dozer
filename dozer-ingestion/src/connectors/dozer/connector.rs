use dozer_log::{
    errors::ReaderError,
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
    models::api_config::AppGrpcOptions,
    serde_json,
    types::SourceDefinition,
};
use tokio::task::JoinSet;
use tonic::{async_trait, transport::Channel};

use crate::{
    connectors::{
        CdcType, Connector, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
    },
    errors::{ConnectorError, NestedDozerConnectorError},
    ingestion::Ingestor,
};

#[derive(Debug)]
pub struct NestedDozerConnector {
    config: NestedDozerConfig,
}

impl NestedDozerConnector {}

#[async_trait]
impl Connector for NestedDozerConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        let _ = Self::get_client(&self.config).await?;

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

    async fn validate_tables(&self, _tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        self.validate_connection().await?;

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

            match log_reader {
                Ok(log_reader) => {
                    let mut schema = log_reader.schema.schema.clone();
                    for field in schema.fields.iter_mut() {
                        // All fields are dynamically defined by the upstream instance
                        field.source = SourceDefinition::Dynamic;
                    }
                    schemas.push(Ok(SourceSchema::new(schema, CdcType::FullChanges)));
                }
                Err(e) => {
                    schemas.push(Err(e));
                }
            }
        }

        Ok(schemas)
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        let mut joinset = JoinSet::new();
        for (table_index, table) in tables.into_iter().enumerate() {
            let builder = self.get_reader_builder(table.name.clone()).await?;
            joinset.spawn(read_table(table_index, builder, ingestor.clone()));
        }

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
        config: &NestedDozerConfig,
    ) -> Result<InternalPipelineServiceClient<Channel>, ConnectorError> {
        let app_server_addr =
            Self::get_server_addr(config.grpc.as_ref().expect("grpc is required"));
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
        let mut client = Self::get_client(&self.config).await?;

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

    fn get_server_addr(config: &AppGrpcOptions) -> String {
        format!("http://{}:{}", config.host, config.port)
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
        let app_server_addr =
            Self::get_server_addr(self.config.grpc.as_ref().expect("grpc is required"));

        let log_options = match self.config.log_options.as_ref() {
            Some(opts) => opts.clone(),
            None => default_log_options().unwrap(),
        };
        let log_options = Self::get_log_options(endpoint, log_options);
        let log_reader_builder = LogReaderBuilder::new(app_server_addr, log_options)
            .await
            .map_err(|e| {
                NestedDozerConnectorError::ReaderError(ReaderError::ReaderBuilderError(e))
            })?;
        Ok(log_reader_builder)
    }
}

async fn read_table(
    table_idx: usize,
    reader_builder: LogReaderBuilder,
    ingestor: Ingestor,
) -> Result<(), ConnectorError> {
    let mut reader = reader_builder.build(0, None);
    loop {
        let (op, seq_no) = reader.next_op().await.map_err(|e| {
            ConnectorError::NestedDozerConnectorError(NestedDozerConnectorError::ReaderError(e))
        })?;
        let msg = match op {
            LogOperation::Op { op } => IngestionMessage::new_op(0, seq_no, table_idx, op),
            LogOperation::Commit { .. } | LogOperation::SnapshottingDone { .. } => continue,
            LogOperation::Terminate => {
                return Err(ConnectorError::NestedDozerConnectorError(
                    NestedDozerConnectorError::TerminateError,
                ))
            }
        };

        ingestor
            .handle_message(msg)
            .map_err(ConnectorError::IngestorError)?;
    }
}
