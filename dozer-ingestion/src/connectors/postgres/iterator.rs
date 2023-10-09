use crate::connectors::ListOrFilterColumns;
use crate::errors::{ConnectorError, PostgresConnectorError};
use crate::ingestion::Ingestor;
use dozer_types::log::debug;
use dozer_types::models::ingestion_types::IngestionMessage;

use std::str::FromStr;

use std::sync::Arc;

use crate::connectors::postgres::connection::helper;
use crate::connectors::postgres::connector::REPLICATION_SLOT_PREFIX;
use crate::connectors::postgres::replication_slot_helper::ReplicationSlotHelper;
use crate::connectors::postgres::replicator::CDCHandler;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::errors::PostgresConnectorError::{
    InvalidQueryError, LSNNotStoredError, LsnNotReturnedFromReplicationSlot, LsnParseError,
};
use postgres_types::PgLsn;

use super::schema::helper::PostgresTableInfo;

pub struct Details {
    name: String,
    publication_name: String,
    slot_name: String,
    tables: Vec<PostgresTableInfo>,
    replication_conn_config: tokio_postgres::Config,
    conn_config: tokio_postgres::Config,
    schema: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicationState {
    Pending,
    SnapshotInProgress,
    Replicating,
}

pub struct PostgresIterator<'a> {
    details: Arc<Details>,
    ingestor: &'a Ingestor,
}

impl<'a> PostgresIterator<'a> {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        publication_name: String,
        slot_name: String,
        tables: Vec<PostgresTableInfo>,
        replication_conn_config: tokio_postgres::Config,
        ingestor: &'a Ingestor,
        conn_config: tokio_postgres::Config,
        schema: Option<String>,
    ) -> Self {
        let details = Arc::new(Details {
            name,
            publication_name,
            slot_name,
            tables,
            replication_conn_config,
            conn_config,
            schema,
        });
        PostgresIterator { details, ingestor }
    }
}

impl<'a> PostgresIterator<'a> {
    pub async fn start(self, lsn: Option<(PgLsn, u64)>) -> Result<(), ConnectorError> {
        let state = ReplicationState::Pending;
        let details = self.details.clone();

        let mut stream_inner = PostgresIteratorHandler {
            details,
            ingestor: self.ingestor,
            state,
            lsn,
        };
        stream_inner.start().await
    }
}

pub struct PostgresIteratorHandler<'a> {
    pub details: Arc<Details>,
    pub lsn: Option<(PgLsn, u64)>,
    pub state: ReplicationState,
    pub ingestor: &'a Ingestor,
}

impl<'a> PostgresIteratorHandler<'a> {
    /*
     Replication involves 3 states
        1) Pending
        - Initialize a replication slot.
        - Initialize snapshots

        2) SnapshotInProgress
        - Sync initial snapshots of specified tables
        - Commit with lsn

        3) Replicating
        - Replicate CDC events using lsn
    */
    pub async fn start(&mut self) -> Result<(), ConnectorError> {
        let details = Arc::clone(&self.details);
        let replication_conn_config = details.replication_conn_config.to_owned();
        let mut client = helper::connect(replication_conn_config)
            .await
            .map_err(ConnectorError::PostgresConnectorError)?;

        // TODO: Handle cases:
        // - When snapshot replication is not completed
        // - When there is gap between available lsn (in case when slot dropped and new created) and last lsn
        // - When publication tables changes

        // We clear inactive replication slots before starting replication
        ReplicationSlotHelper::clear_inactive_slots(&mut client, REPLICATION_SLOT_PREFIX)
            .await
            .map_err(ConnectorError::PostgresConnectorError)?;

        if self.lsn.is_none() {
            debug!("\nCreating Slot....");
            let slot_exist =
                ReplicationSlotHelper::replication_slot_exists(&mut client, &details.slot_name)
                    .await
                    .map_err(ConnectorError::PostgresConnectorError)?;

            if slot_exist {
                // We dont have lsn, so we need to drop replication slot and start from scratch
                ReplicationSlotHelper::drop_replication_slot(&mut client, &details.slot_name)
                    .await
                    .map_err(InvalidQueryError)?;
            }

            client
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .await
                .map_err(|_e| {
                    debug!("failed to begin txn for replication");
                    PostgresConnectorError::BeginReplication
                })?;

            let replication_slot_lsn =
                ReplicationSlotHelper::create_replication_slot(&mut client, &details.slot_name)
                    .await?;
            if let Some(lsn) = replication_slot_lsn {
                let parsed_lsn =
                    PgLsn::from_str(&lsn).map_err(|_| LsnParseError(lsn.to_string()))?;
                self.lsn = Some((parsed_lsn, 0));
            } else {
                return Err(ConnectorError::PostgresConnectorError(
                    LsnNotReturnedFromReplicationSlot,
                ));
            }

            self.state = ReplicationState::SnapshotInProgress;

            /* #####################        SnapshotInProgress         ###################### */
            debug!("\nInitializing snapshots...");

            let snapshotter = PostgresSnapshotter {
                conn_config: details.conn_config.to_owned(),
                ingestor: self.ingestor,
                schema: details.schema.clone(),
            };
            let tables = details
                .tables
                .iter()
                .map(|table_info| ListOrFilterColumns {
                    name: table_info.name.clone(),
                    columns: Some(table_info.columns.clone()),
                    schema: Some(table_info.schema.clone()),
                })
                .collect::<Vec<_>>();
            snapshotter.sync_tables(&tables).await?;

            self.ingestor
                .handle_message(IngestionMessage::SnapshottingDone)
                .await
                .map_err(|_| ConnectorError::IngestorError)?;

            debug!("\nInitialized with tables: {:?}", details.tables);

            client.simple_query("COMMIT;").await.map_err(|_e| {
                debug!("failed to commit txn for replication");
                ConnectorError::PostgresConnectorError(PostgresConnectorError::CommitReplication)
            })?;
        }

        self.state = ReplicationState::Replicating;

        /*  ####################        Replicating         ######################  */
        self.replicate().await
    }

    async fn replicate(&self) -> Result<(), ConnectorError> {
        let (lsn, offset) = self
            .lsn
            .as_ref()
            .map_or(Err(LSNNotStoredError), |(x, offset)| Ok((x, offset)))?;

        let publication_name = self.details.publication_name.clone();
        let slot_name = self.details.slot_name.clone();
        let tables = self.details.tables.clone();
        let mut replicator = CDCHandler {
            replication_conn_config: self.details.replication_conn_config.clone(),
            ingestor: self.ingestor,
            start_lsn: *lsn,
            begin_lsn: 0,
            offset_lsn: 0,
            offset: *offset,
            publication_name,
            slot_name,
            last_commit_lsn: 0,
            seq_no: 0,
            name: self.details.name.clone(),
        };
        replicator.start(tables).await
    }
}
