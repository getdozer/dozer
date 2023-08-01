use dozer_log::{
    errors::ReaderError,
    reader::{LogReaderBuilder, LogReaderOptions},
    schemas::BuildSchema,
};
use dozer_types::{
    errors::types::DeserializationError,
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient,
        DescribeApplicationRequest, DescribeApplicationResponse,
    },
    ingestion_types::{IngestionMessage, NestedDozerConfig, NestedDozerLogOptions},
    models::api_config::AppGrpcOptions,
    serde_json,
};
use futures::future::join_all;
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
        todo!()
    }

    async fn list_columns(
        &self,
        _tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let mut tables = vec![];
        let response = self.describe_application().await?;
        for (endpoint, build) in response.endpoints {
            let schema: BuildSchema = serde_json::from_str(&build.schema_string).map_err(|e| {
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
            let log_reader = self.get_reader(table_info.name.clone()).await;

            match log_reader {
                Ok(log_reader) => {
                    let schema = log_reader.schema.schema.clone();
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
        let mut futures = Vec::with_capacity(tables.len());
        for (table_index, table) in tables.iter().enumerate() {
            futures.push(self.read_table(table_index, table, ingestor));
        }

        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<(), ConnectorError>>()
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

    async fn get_reader(&self, endpoint: String) -> Result<LogReaderBuilder, ConnectorError> {
        let app_server_addr =
            Self::get_server_addr(self.config.grpc.as_ref().expect("grpc is required"));

        let log_options = match self.config.log_options.as_ref() {
            Some(opts) => opts.clone(),
            None => Default::default(),
        };
        let log_options = Self::get_log_options(endpoint, log_options);
        let log_reader_builder = LogReaderBuilder::new(app_server_addr, log_options)
            .await
            .map_err(|e| {
                NestedDozerConnectorError::ReaderError(ReaderError::ReaderBuilderError(e))
            })?;
        Ok(log_reader_builder)
    }

    async fn read_table(
        &self,
        table_index: usize,
        table_info: &TableInfo,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError> {
        let log_reader = self.get_reader(table_info.name.clone()).await;
        let log_reader = log_reader?;
        let mut reader = log_reader.build(0, None);

        loop {
            let msg = reader.next_op().await;
            match msg {
                Ok((op, seq_no)) => {
                    let msg = match op {
                        dozer_log::replication::LogOperation::Op { op } => {
                            Some(IngestionMessage::new_op(0, seq_no, table_index, op))
                        }
                        dozer_log::replication::LogOperation::Commit { epoch: _ } => None,
                        dozer_log::replication::LogOperation::SnapshottingDone {
                            connection_name: _,
                        } => None,
                        dozer_log::replication::LogOperation::Terminate => {
                            return Err(ConnectorError::NestedDozerConnectorError(
                                NestedDozerConnectorError::TerminateError,
                            ))
                        }
                    };

                    if let Some(msg) = msg {
                        ingestor
                            .handle_message(msg)
                            .map_err(ConnectorError::IngestorError)?;
                    }
                }
                Err(e) => {
                    return Err(ConnectorError::NestedDozerConnectorError(
                        NestedDozerConnectorError::ReaderError(e),
                    ))
                }
            }
        }
    }
}
