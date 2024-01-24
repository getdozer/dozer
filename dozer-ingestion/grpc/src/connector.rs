use std::fmt::Debug;
use std::path::Path;

use crate::Error;

use super::adapter::{GrpcIngestor, IngestAdapter};
use super::ingest::IngestorServiceImpl;
use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::utils::TableNotFound;
use dozer_ingestion_connector::{
    async_trait, dozer_types,
    dozer_types::{
        errors::internal::BoxedError,
        grpc_types::ingest::ingest_service_server::IngestServiceServer,
        log::{info, warn},
        models::ingestion_types::{
            default_ingest_host, default_ingest_port, GrpcConfig, GrpcConfigSchemas,
        },
        tonic::transport::Server,
        tracing::Level,
    },
    Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
};
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
    pub fn new(name: String, config: GrpcConfig) -> Self {
        Self {
            name,
            config,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn parse_config(config: &GrpcConfig) -> Result<String, Error>
    where
        T: IngestAdapter,
    {
        let schemas = &config.schemas;
        let schemas_str = match schemas {
            GrpcConfigSchemas::Inline(schemas_str) => schemas_str.clone(),
            GrpcConfigSchemas::Path(path) => {
                let path = Path::new(path);
                std::fs::read_to_string(path)
                    .map_err(|e| Error::CannotReadFile(path.to_path_buf(), e))?
            }
        };

        Ok(schemas_str)
    }

    pub async fn serve(&self, ingestor: &Ingestor, tables: Vec<TableInfo>) -> Result<(), Error> {
        let host = self.config.host.clone().unwrap_or_else(default_ingest_host);
        let port = self.config.port.unwrap_or_else(default_ingest_port);

        let addr = format!("{host}:{port}").parse()?;

        let schemas_str = Self::parse_config(&self.config)?;
        let adapter = GrpcIngestor::<T>::new(schemas_str)?;

        // Ingestor will live as long as the server
        // Refactor to use Arc
        let ingestor = unsafe { std::mem::transmute::<&'_ Ingestor, &'static Ingestor>(ingestor) };

        let ingest_service = IngestorServiceImpl::new(adapter, ingestor, tables);
        let ingest_service = tonic_web::enable(IngestServiceServer::new(ingest_service));

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
            .map_err(Into::into)
    }
}

impl<T: IngestAdapter> GrpcConnector<T> {
    fn get_all_schemas(&self) -> Result<Vec<(String, SourceSchema)>, Error> {
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

    async fn validate_connection(&self) -> Result<(), BoxedError> {
        self.get_all_schemas()?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, BoxedError> {
        Ok(self
            .get_all_schemas()?
            .into_iter()
            .map(|(name, _)| TableIdentifier::from_table_name(name))
            .collect())
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let schemas = self.get_all_schemas()?;
        for table in tables {
            if !schemas
                .iter()
                .any(|(name, _)| name == &table.name && table.schema.is_none())
            {
                return Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
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
                return Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into());
            }
        }
        Ok(result)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
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
                result.push(Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into()));
            }
        }

        Ok(result)
    }

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        _last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        self.serve(ingestor, tables).await.map_err(Into::into)
    }
}
