use crate::connectors::postgres::connection::validator::{validate_connection, validate_slot};
use crate::connectors::postgres::iterator::PostgresIterator;
use crate::connectors::{Connector, TableInfo, ValidationResults};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::tracing::{error, info};
use dozer_types::types::SourceSchema;
use postgres::Client;
use postgres_types::PgLsn;

use crate::connectors::postgres::schema::helper::SchemaHelper;
use crate::errors::ConnectorError::PostgresConnectorError;
use crate::errors::PostgresConnectorError::{CreatePublicationError, DropPublicationError};
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::Config;

use super::connection::helper;

#[derive(Clone, Debug)]
pub struct PostgresConfig {
    pub name: String,
    pub tables: Option<Vec<TableInfo>>,
    pub config: Config,
}

#[derive(Debug)]
pub struct PostgresConnector {
    pub id: u64,
    name: String,
    tables: Option<Vec<TableInfo>>,
    replication_conn_config: Config,
    conn_config: Config,
    schema_helper: SchemaHelper,
}

#[derive(Debug)]
pub struct ReplicationSlotInfo {
    pub name: String,
    pub start_lsn: PgLsn,
}

impl PostgresConnector {
    pub fn new(id: u64, config: PostgresConfig) -> PostgresConnector {
        let mut replication_conn_config = config.config.clone();
        replication_conn_config.replication_mode(ReplicationMode::Logical);

        let helper = SchemaHelper::new(config.config.clone(), None);

        // conn_str - replication_conn_config
        // conn_str_plain- conn_config

        PostgresConnector {
            id,
            name: config.name,
            conn_config: config.config,
            replication_conn_config,
            tables: config.tables,
            schema_helper: helper,
        }
    }

    fn get_lsn_with_offset_from_seq(
        conn_name: String,
        from_seq: Option<(u64, u64)>,
    ) -> Option<(PgLsn, u64)> {
        from_seq.map_or_else(
            || {
                info!("[{}] Starting replication", conn_name);
                None
            },
            |(lsn, checkpoint)| {
                info!(
                    "[{}] Starting replication from checkpoint ({}/{})",
                    conn_name, lsn, checkpoint
                );
                Some((PgLsn::from(lsn), checkpoint))
            },
        )
    }
}

impl Connector for PostgresConnector {
    fn get_schemas(
        &self,
        table_names: Option<&Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        self.schema_helper
            .get_schemas(table_names.map(|t| &t[..]))
            .map_err(PostgresConnectorError)
    }

    fn start(
        &self,
        from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
    ) -> Result<(), ConnectorError> {
        let client = helper::connect(self.replication_conn_config.clone())
            .map_err(PostgresConnectorError)?;
        self.create_publication(client)?;

        let lsn = PostgresConnector::get_lsn_with_offset_from_seq(self.name.clone(), from_seq);

        let iterator = PostgresIterator::new(
            self.id,
            self.name.clone(),
            self.get_publication_name(),
            self.get_slot_name(),
            self.schema_helper.get_tables(Some(&tables))?,
            self.replication_conn_config.clone(),
            ingestor,
            self.conn_config.clone(),
        );
        iterator.start(lsn)
    }

    fn validate(&self, tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        let tables_list = tables.or_else(|| self.tables.clone());
        validate_connection(
            &self.name,
            self.conn_config.clone(),
            tables_list.as_ref(),
            None,
        )?;

        Ok(())
    }

    fn validate_schemas(&self, tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        SchemaHelper::validate(&self.schema_helper, tables).map_err(PostgresConnectorError)
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        let tables = self.schema_helper.get_tables(None)?;
        Ok(tables
            .into_iter()
            .map(|table_info| TableInfo {
                name: table_info.name,
                columns: Some(table_info.columns),
            })
            .collect())
    }

    fn can_start_from(&self, (lsn, _): (u64, u64)) -> Result<bool, ConnectorError> {
        let mut client =
            helper::connect(self.conn_config.clone()).map_err(PostgresConnectorError)?;
        let slot_info = ReplicationSlotInfo {
            name: self.get_slot_name(),
            start_lsn: PgLsn::from(lsn),
        };
        match validate_slot(&mut client, &slot_info, self.tables.as_ref()) {
            Ok(_) => Ok(true),
            Err(_e) => Ok(false),
        }
    }
}

impl PostgresConnector {
    fn get_publication_name(&self) -> String {
        format!("dozer_publication_{}", self.name)
    }

    pub fn get_slot_name(&self) -> String {
        format!("dozer_slot_{}", self.name)
    }

    pub fn create_publication(&self, mut client: Client) -> Result<(), ConnectorError> {
        let publication_name = self.get_publication_name();
        let table_str: String = match self.tables.as_ref() {
            None => "ALL TABLES".to_string(),
            Some(arr) => {
                let table_names: Vec<String> = arr.iter().map(|t| t.name.clone()).collect();
                format!("TABLE {}", table_names.join(" , "))
            }
        };

        client
            .simple_query(format!("DROP PUBLICATION IF EXISTS {publication_name}").as_str())
            .map_err(|e| {
                error!("failed to drop publication {}", e.to_string());
                DropPublicationError
            })?;

        client
            .simple_query(format!("CREATE PUBLICATION {publication_name} FOR {table_str}").as_str())
            .map_err(|e| {
                error!("failed to create publication {}", e.to_string());
                CreatePublicationError
            })?;
        Ok(())
    }
}
