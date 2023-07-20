use std::fmt::Debug;
use std::path::Path;

use super::adapter::{GrpcIngestor, IngestAdapter};
use super::ingest::IngestorServiceImpl;
use crate::connectors::{table_name, SourceSchema, SourceSchemaResult, TableIdentifier};
use crate::{
    connectors::{Connector, TableInfo},
    errors::ConnectorError,
    ingestion::Ingestor,
};
use dozer_types::grpc_types::ingest::ingest_service_server::IngestServiceServer;
use dozer_types::ingestion_types::GrpcConfig;
use dozer_types::log::{info, warn};
use dozer_types::tracing::Level;
use tonic::async_trait;
use tonic::transport::Server;
use tower_http::trace::{self, TraceLayer};

#[derive(Debug)]
pub struct GrpcConnector<T>
where
    T: IngestAdapter,
{
    pub name: String,
    pub config: GrpcConfig,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> GrpcConnector<T>
where
    T: IngestAdapter,
{
    pub fn new(name: String, config: GrpcConfig) -> Result<Self, ConnectorError> {
        Ok(Self {
            name,
            config,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn parse_config(config: &GrpcConfig) -> Result<String, ConnectorError>
    where
        T: IngestAdapter,
    {
        let schemas = config.schemas.as_ref().map_or_else(
            || {
                Err(ConnectorError::InitializationError(
                    "schemas not found".to_string(),
                ))
            },
            Ok,
        )?;
        let schemas_str = match schemas {
            dozer_types::ingestion_types::GrpcConfigSchemas::Inline(schemas_str) => {
                schemas_str.clone()
            }
            dozer_types::ingestion_types::GrpcConfigSchemas::Path(path) => {
                let path = Path::new(path);
                std::fs::read_to_string(path)
                    .map_err(|e| ConnectorError::InitializationError(e.to_string()))?
            }
        };

        Ok(schemas_str)
    }

    pub async fn serve(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        let host = &self.config.host;
        let port = self.config.port;

        let addr = format!("{host:}:{port:}").parse().map_err(|e| {
            ConnectorError::InitializationError(format!("Failed to parse address: {e}"))
        })?;

        let schemas_str = Self::parse_config(&self.config)?;
        let adapter = GrpcIngestor::<T>::new(schemas_str)?;

        // Ingestor will live as long as the server
        // Refactor to use Arc
        let ingestor = unsafe { std::mem::transmute::<&'_ Ingestor, &'static Ingestor>(ingestor) };

        let ingest_service = IngestorServiceImpl::new(adapter, ingestor, tables);
        let ingest_service = tonic_web::config()
            .allow_all_origins()
            .enable(IngestServiceServer::new(ingest_service));

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(
                dozer_types::grpc_types::ingest::FILE_DESCRIPTOR_SET,
            )
            .build()
            .unwrap();
        info!("Starting Dozer GRPC Ingestor  on http://{}:{} ", host, port,);
        Server::builder()
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
            )
            .accept_http1(true)
            .add_service(ingest_service)
            .add_service(reflection_service)
            .serve(addr)
            .await
            .map_err(|e| ConnectorError::InitializationError(e.to_string()))
    }
}

impl<T: IngestAdapter> GrpcConnector<T> {
    fn get_all_schemas(&self) -> Result<Vec<(String, SourceSchema)>, ConnectorError> {
        let schemas_str = Self::parse_config(&self.config)?;
        let adapter = GrpcIngestor::<T>::new(schemas_str)?;
        adapter.get_schemas()
    }
}

#[async_trait]
impl<T> Connector for GrpcConnector<T>
where
    T: IngestAdapter,
{
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        self.get_all_schemas().map(|_| ())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        Ok(self
            .get_all_schemas()?
            .into_iter()
            .map(|(name, _)| TableIdentifier::from_table_name(name))
            .collect())
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        let schemas = self.get_all_schemas()?;
        for table in tables {
            if !schemas
                .iter()
                .any(|(name, _)| name == &table.name && table.schema.is_none())
            {
                return Err(ConnectorError::TableNotFound(table_name(
                    table.schema.as_deref(),
                    &table.name,
                )));
            }
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let schemas = self.get_all_schemas()?;
        let mut result = vec![];
        for table in tables {
            if let Some((_, schema)) = schemas
                .iter()
                .find(|(name, _)| name == &table.name && table.schema.is_none())
            {
                let column_names = schema
                    .schema
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect();
                result.push(TableInfo {
                    schema: table.schema,
                    name: table.name,
                    column_names,
                })
            } else {
                return Err(ConnectorError::TableNotFound(table_name(
                    table.schema.as_deref(),
                    &table.name,
                )));
            }
        }
        Ok(result)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        let schemas_str = Self::parse_config(&self.config)?;
        let adapter = GrpcIngestor::<T>::new(schemas_str)?;

        let schemas = adapter.get_schemas()?;

        let mut result = vec![];
        for table in table_infos {
            if let Some((_, schema)) = schemas
                .iter()
                .find(|(name, _)| name == &table.name && table.schema.is_none())
            {
                warn!("TODO: filter columns");
                result.push(Ok(schema.clone()));
            } else {
                result.push(Err(ConnectorError::TableNotFound(table_name(
                    table.schema.as_deref(),
                    &table.name,
                ))));
            }
        }

        Ok(result)
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        self.serve(ingestor, tables).await
    }
}
