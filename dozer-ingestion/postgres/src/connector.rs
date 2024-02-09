use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{errors::internal::BoxedError, types::FieldType},
    utils::ListOrFilterColumns,
    Connector, Ingestor, SourceSchemaResult, TableIdentifier, TableInfo,
};
use postgres_types::PgLsn;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::Config;

use crate::{
    connection::validator::validate_connection,
    iterator::PostgresIterator,
    schema::helper::{SchemaHelper, DEFAULT_SCHEMA_NAME},
    PostgresConnectorError,
};

use super::connection::client::Client;
use super::connection::helper;

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub name: String,
    pub config: Config,
    pub schema: Option<String>,
    pub batch_size: usize,
}

#[derive(Debug)]
pub struct PostgresConnector {
    pub name: String,
    pub slot_name: String,
    replication_conn_config: Config,
    conn_config: Config,
    schema_helper: SchemaHelper,
    pub schema: Option<String>,
    batch_size: usize,
}

#[derive(Debug)]
pub struct ReplicationSlotInfo {
    pub name: String,
    pub start_lsn: PgLsn,
}

pub const REPLICATION_SLOT_PREFIX: &str = "dozer_slot";

impl PostgresConnector {
    pub fn new(
        config: PostgresConfig,
        state: Option<Vec<u8>>,
    ) -> Result<PostgresConnector, PostgresConnectorError> {
        let mut replication_conn_config = config.config.clone();
        replication_conn_config.replication_mode(ReplicationMode::Logical);

        let helper = SchemaHelper::new(config.config.clone(), config.schema.clone());

        // conn_str - replication_conn_config
        // conn_str_plain- conn_config

        let slot_name = state
            .map(String::from_utf8)
            .transpose()?
            .unwrap_or_else(|| get_slot_name(&config.name));

        Ok(PostgresConnector {
            name: config.name,
            slot_name,
            conn_config: config.config,
            replication_conn_config,
            schema_helper: helper,
            schema: config.schema,
            batch_size: config.batch_size,
        })
    }
}

#[async_trait]
impl Connector for PostgresConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&mut self) -> Result<(), BoxedError> {
        validate_connection(&self.name, self.conn_config.clone(), None, None)
            .await
            .map_err(Into::into)
    }

    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError> {
        Ok(self
            .schema_helper
            .get_tables(None)
            .await?
            .into_iter()
            .map(|table| TableIdentifier::new(Some(table.schema), table.name))
            .collect())
    }

    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let tables = tables
            .iter()
            .map(|table| ListOrFilterColumns {
                schema: table.schema.clone(),
                name: table.name.clone(),
                columns: None,
            })
            .collect::<Vec<_>>();
        validate_connection(&self.name, self.conn_config.clone(), Some(&tables), None)
            .await
            .map_err(Into::into)
    }

    async fn list_columns(
        &mut self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let table_infos = tables
            .iter()
            .map(|table| ListOrFilterColumns {
                schema: table.schema.clone(),
                name: table.name.clone(),
                columns: None,
            })
            .collect::<Vec<_>>();
        Ok(self
            .schema_helper
            .get_tables(Some(&table_infos))
            .await?
            .into_iter()
            .map(|table| TableInfo {
                schema: Some(table.schema),
                name: table.name,
                column_names: table.columns,
            })
            .collect())
    }

    async fn get_schemas(
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let table_infos = table_infos
            .iter()
            .map(|table| ListOrFilterColumns {
                schema: table.schema.clone(),
                name: table.name.clone(),
                columns: Some(table.column_names.clone()),
            })
            .collect::<Vec<_>>();
        Ok(self
            .schema_helper
            .get_schemas(&table_infos)
            .await?
            .into_iter()
            .map(|schema_result| schema_result.map_err(Into::into))
            .collect())
    }

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(self.slot_name.as_bytes().to_vec())
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let lsn = last_checkpoint.map(|checkpoint| checkpoint.txid.into());

        if lsn.is_none() {
            let client = helper::connect(self.replication_conn_config.clone()).await?;
            let table_identifiers = tables
                .iter()
                .map(|table| TableIdentifier::new(table.schema.clone(), table.name.clone()))
                .collect::<Vec<_>>();
            create_publication(client, &self.name, Some(&table_identifiers)).await?;
        }

        let tables = tables
            .into_iter()
            .map(|table| ListOrFilterColumns {
                schema: table.schema,
                name: table.name,
                columns: Some(table.column_names),
            })
            .collect::<Vec<_>>();
        let iterator = PostgresIterator::new(
            self.name.clone(),
            get_publication_name(&self.name),
            self.slot_name.clone(),
            self.schema_helper.get_tables(Some(&tables)).await?,
            self.replication_conn_config.clone(),
            ingestor,
            self.conn_config.clone(),
            self.schema.clone(),
            self.batch_size,
        );
        iterator.start(lsn).await.map_err(Into::into)
    }
}

fn get_publication_name(conn_name: &str) -> String {
    format!("dozer_publication_{}", conn_name)
}

pub fn get_slot_name(conn_name: &str) -> String {
    let rand_name_suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect();

    format!(
        "{REPLICATION_SLOT_PREFIX}_{}_{}",
        conn_name,
        rand_name_suffix.to_lowercase()
    )
}

pub async fn create_publication(
    mut client: Client,
    conn_name: &str,
    table_identifiers: Option<&[TableIdentifier]>,
) -> Result<(), PostgresConnectorError> {
    let publication_name = get_publication_name(conn_name);
    let table_str: String = match table_identifiers {
        None => "ALL TABLES".to_string(),
        Some(table_identifiers) => {
            let table_names = table_identifiers
                .iter()
                .map(|table_identifier| {
                    format!(
                        r#""{}"."{}""#,
                        table_identifier
                            .schema
                            .as_deref()
                            .unwrap_or(DEFAULT_SCHEMA_NAME),
                        table_identifier.name
                    )
                })
                .collect::<Vec<_>>();
            format!("TABLE {}", table_names.join(" , "))
        }
    };

    client
        .simple_query(format!("DROP PUBLICATION IF EXISTS {publication_name}").as_str())
        .await
        .map_err(PostgresConnectorError::DropPublicationError)?;

    client
        .simple_query(format!("CREATE PUBLICATION {publication_name} FOR {table_str}").as_str())
        .await
        .map_err(PostgresConnectorError::CreatePublicationError)?;

    Ok(())
}
