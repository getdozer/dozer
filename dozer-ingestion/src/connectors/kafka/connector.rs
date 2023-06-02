use kafka::client::KafkaClient;
use crate::connectors::{Connector, SourceSchema, SourceSchemaResult, TableIdentifier};
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::KafkaConfig;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use tonic::async_trait;

use crate::connectors::kafka::debezium::no_schema_registry::NoSchemaRegistry;
use crate::connectors::kafka::debezium::schema_registry::SchemaRegistry;
use crate::connectors::kafka::debezium::stream_consumer::DebeziumStreamConsumer;
use crate::connectors::kafka::stream_consumer::StreamConsumer;
use crate::errors::DebeziumError::{DebeziumConnectionError, TopicNotDefined};

#[derive(Debug)]
pub struct KafkaConnector {
    pub id: u64,
    config: KafkaConfig,
}

impl KafkaConnector {
    pub fn new(id: u64, config: KafkaConfig) -> Self {
        Self { id, config }
    }

    async fn get_schemas_impl(
        &self,
        table_names: Option<&[String]>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        if let Some(schema_registry_url) = &self.config.schema_registry_url {
            SchemaRegistry::get_schema(table_names, schema_registry_url.clone()).await
        } else {
            NoSchemaRegistry::get_schema(table_names, self.config.broker.clone())
        }
    }
}

#[async_trait]
impl Connector for KafkaConnector {
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
        let mut client = KafkaClient::new(vec![self.config.broker.clone()]);
        client.load_metadata_all().unwrap();
        let topics = client.topics();

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

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        let topic = tables
            .get(0)
            .map_or(Err(TopicNotDefined), |table| Ok(&table.name))?;

        let broker = self.config.broker.to_owned();
        run(broker, topic, ingestor).await
    }
}

async fn run(broker: String, topic: &str, ingestor: &Ingestor) -> Result<(), ConnectorError> {
    let con = Consumer::from_hosts(vec![broker])
        .with_topic(topic.to_string())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .map_err(DebeziumConnectionError)?;

    let consumer = DebeziumStreamConsumer::default();
    consumer.run(con, ingestor)
}
