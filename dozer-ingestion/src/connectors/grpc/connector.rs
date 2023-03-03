use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use super::adapter::{get_adapter, GrpcIngestAdapter};
use super::ingest::IngestorServiceImpl;
use crate::connectors::ValidationResults;
use crate::{
    connectors::{Connector, TableInfo},
    errors::ConnectorError,
    ingestion::Ingestor,
};
use dozer_types::grpc_types::ingest::ingest_service_server::IngestServiceServer;
use dozer_types::ingestion_types::GrpcConfig;
use dozer_types::log::info;
use dozer_types::types::{Schema, SchemaIdentifier, SourceSchema};
use tonic::transport::Server;
use tower_http::trace::TraceLayer;

pub struct GrpcConnector<T> {
    pub id: u64,
    pub name: String,
    pub config: GrpcConfig,
    pub adapter: Arc<Box<dyn GrpcIngestAdapter<T>>>,
}
impl<T> Debug for GrpcConnector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcConnector")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("config", &self.config)
            .finish()
    }
}

impl<T> GrpcConnector<T> {
    pub fn new(id: u64, name: String, config: GrpcConfig) -> Result<Self, ConnectorError> {
        let adapter = Arc::new(get_adapter(&config.adapter)?);
        Ok(Self {
            id,
            name,
            config,
            adapter,
        })
    }

    pub fn parse_schemas(
        adapter: Arc<Box<dyn GrpcIngestAdapter<T>>>,
        config: &GrpcConfig,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
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

        adapter.get_schemas(&schemas_str)
    }

    fn get_schema_map(
        adapter: Arc<Box<dyn GrpcIngestAdapter<T>>>,
        config: &GrpcConfig,
    ) -> Result<HashMap<String, Schema>, ConnectorError> {
        let schemas = Self::parse_schemas(adapter, config)?;
        Ok(schemas
            .into_iter()
            .enumerate()
            .map(|(id, mut v)| {
                v.schema.identifier = Some(SchemaIdentifier {
                    id: id as u32,
                    version: 1,
                });
                (v.name, v.schema)
            })
            .collect())
    }

    pub fn serve(&self, ingestor: &Ingestor) -> Result<(), ConnectorError> {
        let host = &self.config.host;
        let port = self.config.port;

        let addr = format!("{host:}:{port:}").parse().map_err(|e| {
            ConnectorError::InitializationError(format!("Failed to parse address: {e}"))
        })?;
        let rt = tokio::runtime::Runtime::new().expect("Failed to initialize tokio runtime");
        let schema_map = Self::get_schema_map(self.adapter, &self.config)?;

        let adapter = self.adapter.clone();
        rt.block_on(async {
            // Ingestor will live as long as the server
            // Refactor to use Arc
            let ingestor =
                unsafe { std::mem::transmute::<&'_ Ingestor, &'static Ingestor>(ingestor) };
            let schema_map = schema_map.clone();
            let ingest_service = IngestorServiceImpl::new(&schema_map, ingestor, adapter);
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
                .layer(TraceLayer::new_for_http())
                .accept_http1(true)
                .add_service(ingest_service)
                .add_service(reflection_service)
                .serve(addr)
                .await
        })
        .map_err(|e| ConnectorError::InitializationError(e.to_string()))
    }
}

impl<T> Connector for GrpcConnector<T> {
    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        let schemas = Self::parse_schemas(self.adapter, &self.config)?;
        let schemas = table_names.map_or(schemas.clone(), |names| {
            schemas
                .into_iter()
                .filter(|s| names.iter().any(|n| n.name == s.name))
                .collect()
        });
        Ok(schemas)
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        _tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        self.serve(ingestor)
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        let schemas = Self::parse_schemas(self.adapter, &self.config);
        schemas.map(|_| ())
    }

    fn validate_schemas(&self, tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        let mut results = HashMap::new();
        let schemas = Self::get_schema_map(self.adapter, &self.config)?;
        for table in tables {
            let r = schemas.get(&table.name).map_or(
                Err(ConnectorError::InitializationError(format!(
                    "Schema not found for table {}",
                    table.name
                ))),
                |_| Ok(()),
            );

            results.insert(table.name.clone(), vec![(None, r)]);
        }
        Ok(results)
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        self.get_tables_default(tables)
    }

    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, ConnectorError> {
        Ok(false)
    }
}
