use crate::connectors::TableInfo;

use crate::errors::{ConnectorError, PostgresConnectorError};
use crate::ingestion::Ingestor;
use dozer_types::log::debug;

use std::cell::RefCell;
use std::str::FromStr;

use std::sync::Arc;

use crate::connectors::postgres::connection::helper;
use crate::connectors::postgres::replication_slot_helper::ReplicationSlotHelper;
use crate::connectors::postgres::replicator::CDCHandler;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::errors::PostgresConnectorError::{
    InvalidQueryError, LSNNotStoredError, LsnNotReturnedFromReplicationSlot, LsnParseError,
};
use dozer_types::ingestion_types::IngestionMessage::SnapshottingDone;
use postgres_types::PgLsn;
use tokio::runtime::Runtime;

pub struct Details {
    name: String,
    publication_name: String,
    slot_name: String,
    tables: Vec<TableInfo>,
    replication_conn_config: tokio_postgres::Config,
    conn_config: tokio_postgres::Config,
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
    connector_id: u64,
}

impl<'a> PostgresIterator<'a> {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        id: u64,
        name: String,
        publication_name: String,
        slot_name: String,
        tables: Vec<TableInfo>,
        replication_conn_config: tokio_postgres::Config,
        ingestor: &'a Ingestor,
        conn_config: tokio_postgres::Config,
    ) -> Self {
        let details = Arc::new(Details {
            name,
            publication_name,
            slot_name,
            tables,
            replication_conn_config,
            conn_config,
        });
        PostgresIterator {
            details,
            ingestor,
            connector_id: id,
        }
    }
}

impl<'a> PostgresIterator<'a> {
    pub fn start(self, lsn: Option<(PgLsn, u64)>) -> Result<(), ConnectorError> {
        let lsn = RefCell::new(lsn);
        let state = RefCell::new(ReplicationState::Pending);
        let details = self.details.clone();
        let connector_id = self.connector_id;

        let stream_inner = PostgresIteratorHandler {
            details,
            ingestor: self.ingestor,
            state,
            lsn,
            connector_id,
        };
        stream_inner._start()
    }
}

pub struct PostgresIteratorHandler<'a> {
    pub details: Arc<Details>,
    pub lsn: RefCell<Option<(PgLsn, u64)>>,
    pub state: RefCell<ReplicationState>,
    pub ingestor: &'a Ingestor,
    pub connector_id: u64,
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
    pub fn _start(&self) -> Result<(), ConnectorError> {
        let details = Arc::clone(&self.details);
        let replication_conn_config = details.replication_conn_config.to_owned();
        let client = Arc::new(RefCell::new(
            helper::connect(replication_conn_config)
                .map_err(ConnectorError::PostgresConnectorError)?,
        ));

        // TODO: Handle cases:
        // - When snapshot replication is not completed
        // - When there is gap between available lsn (in case when slot dropped and new created) and last lsn
        // - When publication tables changes
        let mut tables = details.tables.clone();
        if self.lsn.clone().into_inner().is_none() {
            debug!("\nCreating Slot....");
            let slot_exist =
                ReplicationSlotHelper::replication_slot_exists(client.clone(), &details.slot_name)
                    .map_err(ConnectorError::PostgresConnectorError)?;

            if slot_exist {
                // We dont have lsn, so we need to drop replication slot and start from scratch
                ReplicationSlotHelper::drop_replication_slot(client.clone(), &details.slot_name)
                    .map_err(InvalidQueryError)?;
            }

            client
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .map_err(|_e| {
                    debug!("failed to begin txn for replication");
                    PostgresConnectorError::BeginReplication
                })?;

            let replication_slot_lsn =
                ReplicationSlotHelper::create_replication_slot(client.clone(), &details.slot_name)?;
            if let Some(lsn) = replication_slot_lsn {
                let parsed_lsn =
                    PgLsn::from_str(&lsn).map_err(|_| LsnParseError(lsn.to_string()))?;
                self.lsn.replace(Some((parsed_lsn, 0)));
            } else {
                return Err(ConnectorError::PostgresConnectorError(
                    LsnNotReturnedFromReplicationSlot,
                ));
            }

            self.state.replace(ReplicationState::SnapshotInProgress);

            /* #####################        SnapshotInProgress         ###################### */
            debug!("\nInitializing snapshots...");

            let snapshotter = PostgresSnapshotter {
                tables: details.tables.clone(),
                conn_config: details.conn_config.to_owned(),
                ingestor: self.ingestor,
                connector_id: self.connector_id,
            };
            tables = snapshotter.sync_tables(details.tables.clone())?;

            let lsn = self.lsn.borrow().map_or(0, |(lsn, _)| u64::from(lsn));
            self.ingestor
                .handle_message(((lsn, 0), SnapshottingDone))
                .map_err(ConnectorError::IngestorError)?;

            debug!("\nInitialized with tables: {:?}", tables);

            client.borrow_mut().simple_query("COMMIT;").map_err(|_e| {
                debug!("failed to commit txn for replication");
                ConnectorError::PostgresConnectorError(PostgresConnectorError::CommitReplication)
            })?;
        }

        self.state.replace(ReplicationState::Replicating);

        /*  ####################        Replicating         ######################  */
        self.replicate(tables)
    }

    fn replicate(&self, tables: Vec<TableInfo>) -> Result<(), ConnectorError> {
        let rt = Runtime::new().unwrap();
        let lsn = self.lsn.borrow();
        let (lsn, offset) = lsn
            .as_ref()
            .map_or(Err(LSNNotStoredError), |(x, offset)| Ok((x, offset)))?;

        let publication_name = self.details.publication_name.clone();
        let slot_name = self.details.slot_name.clone();
        rt.block_on(async {
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
                connector_id: self.connector_id,
                seq_no: 0,
                name: self.details.name.clone(),
            };
            replicator.start(tables).await
        })
    }
}
