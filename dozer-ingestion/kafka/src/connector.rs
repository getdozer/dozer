use dozer_ingestion_connector::async_trait;
use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::KafkaConfig;
use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::dozer_types::types::FieldType;
use dozer_ingestion_connector::Connector;
use dozer_ingestion_connector::Ingestor;
use dozer_ingestion_connector::SourceSchema;
use dozer_ingestion_connector::SourceSchemaResult;
use dozer_ingestion_connector::TableIdentifier;
use dozer_ingestion_connector::TableInfo;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

use crate::no_schema_registry_basic::NoSchemaRegistryBasic;
use crate::schema_registry_basic::SchemaRegistryBasic;
use crate::stream_consumer::StreamConsumer;
use crate::stream_consumer_basic::StreamConsumerBasic;
use crate::KafkaError;

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
    ) -> Result<Vec<SourceSchema>, KafkaError> {
        if let Some(schema_registry_url) = &self.config.schema_registry_url {
            SchemaRegistryBasic::get_schema(table_names, schema_registry_url.clone()).await
        } else {
            NoSchemaRegistryBasic::get_schema(table_names)
        }
    }
}

#[async_trait]
impl Connector for KafkaConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&mut self) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError> {
        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.config.broker.clone())
            .set("api.version.request", "true")
            .create::<BaseConsumer>()?;

        let metadata =
            consumer.fetch_metadata(None, Timeout::After(std::time::Duration::new(60, 0)))?;
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

    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let table_names = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        self.get_schemas_impl(Some(&table_names)).await?;
        Ok(())
    }

    async fn list_columns(
        &mut self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
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
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
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

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let broker = self.config.broker.to_owned();
        run(
            broker,
            tables,
            last_checkpoint,
            ingestor,
            &self.config.schema_registry_url,
        )
        .await
        .map_err(Into::into)
    }
}

async fn run(
    broker: String,
    tables: Vec<TableInfo>,
    last_checkpoint: Option<OpIdentifier>,
    ingestor: &Ingestor,
    schema_registry_url: &Option<String>,
) -> Result<(), KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", broker)
        .set("group.id", "dozer")
        .set("enable.auto.commit", "true");

    let consumer = StreamConsumerBasic::default();
    consumer
        .run(
            client_config,
            ingestor,
            tables,
            last_checkpoint,
            schema_registry_url,
        )
        .await
}
