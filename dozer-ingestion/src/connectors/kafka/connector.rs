use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;

use crate::connectors::{
    ConnectorMeta, ConnectorStart, SourceSchema, SourceSchemaResult, TableIdentifier, TableToIngest,
};
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;
use rdkafka::consumer::Consumer;
use rdkafka::util::Timeout;

use tonic::async_trait;

use crate::connectors::kafka::no_schema_registry_basic::NoSchemaRegistryBasic;

use crate::connectors::kafka::schema_registry_basic::SchemaRegistryBasic;
use crate::connectors::kafka::stream_consumer::StreamConsumer;
use crate::connectors::kafka::stream_consumer_basic::StreamConsumerBasic;
use crate::errors::KafkaError::KafkaConnectionError;

#[derive(Debug)]
pub struct KafkaConnector {
    config: KafkaConfig,
}

impl KafkaConnector {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    async fn get_schemas_impl(
        &self,
        table_names: Option<&[String]>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        if let Some(schema_registry_url) = &self.config.schema_registry_url {
            SchemaRegistryBasic::get_schema(table_names, schema_registry_url.clone()).await
        } else {
            NoSchemaRegistryBasic::get_schema(table_names)
        }
    }
}

#[async_trait]
impl ConnectorMeta for KafkaConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.broker.clone())
            .set("api.version.request", "true")
            .create::<BaseConsumer>()
            .map_err(KafkaConnectionError)?;

        let metadata = consumer
            .fetch_metadata(None, Timeout::After(std::time::Duration::new(60, 0)))
            .map_err(KafkaConnectionError)?;
        let topics = metadata.topics();

        let mut tables = vec![];
        for topic in topics {
            tables.push(TableIdentifier {
                schema: None,
                name: topic.name().to_string(),
            });
        }

        Ok(tables)
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        let table_names = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        self.get_schemas_impl(Some(&table_names)).await.map(|_| ())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let table_names = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        let schemas = self.get_schemas_impl(Some(&table_names)).await?;
        let mut result = vec![];
        for (table, schema) in tables.into_iter().zip(schemas) {
            let column_names = schema
                .schema
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect();
            result.push(TableInfo {
                schema: table.schema,
                name: table.name,
                column_names,
            });
        }
        Ok(result)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        let table_names = table_infos
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        Ok(self
            .get_schemas_impl(Some(&table_names))
            .await?
            .into_iter()
            .map(Ok)
            .collect())
    }
}

#[async_trait(?Send)]
impl ConnectorStart for KafkaConnector {
    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableToIngest>,
    ) -> Result<(), ConnectorError> {
        let broker = self.config.broker.to_owned();
        run(broker, tables, ingestor, &self.config.schema_registry_url).await
    }
}

async fn run(
    broker: String,
    tables: Vec<TableToIngest>,
    ingestor: &Ingestor,
    schema_registry_url: &Option<String>,
) -> Result<(), ConnectorError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", broker)
        .set("group.id", "dozer")
        .set("enable.auto.commit", "true");

    let consumer = StreamConsumerBasic::default();
    consumer
        .run(client_config, ingestor, tables, schema_registry_url)
        .await
}
