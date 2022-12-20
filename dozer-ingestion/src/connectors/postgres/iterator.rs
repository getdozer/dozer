use crate::connectors::TableInfo;

use crate::errors::{ConnectorError, PostgresConnectorError};
use crate::ingestion::Ingestor;
use dozer_types::log::debug;

use dozer_types::parking_lot::RwLock;
use std::cell::RefCell;

use std::sync::Arc;

use crate::connectors::postgres::connection::helper;
use crate::connectors::postgres::replicator::CDCHandler;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::errors::ConnectorError::UnexpectedQueryMessageError;
use crate::errors::PostgresConnectorError::LSNNotStoredError;
use postgres::Client;
use tokio::runtime::Runtime;
use tokio_postgres::SimpleQueryMessage;

pub struct Details {
    #[allow(dead_code)]
    id: u64,
    publication_name: String,
    slot_name: String,
    tables: Option<Vec<TableInfo>>,
    replication_conn_config: tokio_postgres::Config,
    conn_config: tokio_postgres::Config,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicationState {
    Pending,
    SnapshotInProgress,
    Replicating,
}

pub struct PostgresIterator {
    details: Arc<Details>,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
}

impl PostgresIterator {
    pub fn new(
        id: u64,
        publication_name: String,
        slot_name: String,
        tables: Option<Vec<TableInfo>>,
        replication_conn_config: tokio_postgres::Config,
        ingestor: Arc<RwLock<Ingestor>>,
        conn_config: tokio_postgres::Config,
    ) -> Self {
        let details = Arc::new(Details {
            id,
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

impl PostgresIterator {
    pub fn start(&self) -> Result<(), ConnectorError> {
        let lsn = RefCell::new(self.get_last_lsn_for_connection()?);
        let state = RefCell::new(ReplicationState::Pending);
        let details = self.details.clone();
        let ingestor = self.ingestor.clone();
        let connector_id = self.connector_id;

        let mut stream_inner = PostgresIteratorHandler {
            details,
            ingestor,
            state,
            lsn,
            connector_id,
        };
        stream_inner._start()
    }

    pub fn get_last_lsn_for_connection(&self) -> Result<Option<String>, ConnectorError> {
        // TODO: implement logic of getting last lsn from pipeline
        Ok(None)
    }
}

pub struct PostgresIteratorHandler {
    pub details: Arc<Details>,
    pub lsn: RefCell<Option<String>>,
    pub state: RefCell<ReplicationState>,
    pub ingestor: Arc<RwLock<Ingestor>>,
    pub connector_id: u64,
}

impl PostgresIteratorHandler {
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
    pub fn _start(&mut self) -> Result<(), ConnectorError> {
        let details = Arc::clone(&self.details);
        let replication_conn_config = details.replication_conn_config.to_owned();
        let client = Arc::new(RefCell::new(helper::connect(replication_conn_config)?));

        // TODO: Handle cases:
        // - When snapshot replication is not completed
        // - When there is gap between available lsn (in case when slot dropped and new created) and last lsn
        // - When publication tables changes
        let mut tables = details.tables.clone();
        if self.lsn.clone().into_inner().is_none() {
            debug!("\nCreating Slot....");
            if let Ok(true) = self.replication_slot_exists(client.clone()) {
                // We dont have lsn, so we need to drop replication slot and start from scratch
                self.drop_replication_slot(client.clone());
            }

            client
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
                .map_err(|_e| {
                    debug!("failed to begin txn for replication");
                    PostgresConnectorError::BeginReplication
                })?;

            let replication_slot_lsn = self.create_replication_slot(client.clone())?;

            self.lsn.replace(replication_slot_lsn);
            self.state
                .clone()
                .replace(ReplicationState::SnapshotInProgress);

            /* #####################        SnapshotInProgress         ###################### */
            debug!("\nInitializing snapshots...");

            let snapshotter = PostgresSnapshotter {
                tables: details.tables.clone(),
                conn_config: details.conn_config.to_owned(),
                ingestor: Arc::clone(&self.ingestor),
                connector_id: self.connector_id,
            };
            tables = snapshotter.sync_tables(details.tables.clone())?;

            debug!("\nInitialized with tables: {:?}", tables);

            client.borrow_mut().simple_query("COMMIT;").map_err(|_e| {
                debug!("failed to commit txn for replication");
                ConnectorError::PostgresConnectorError(PostgresConnectorError::CommitReplication)
            })?;
        }

        self.state.clone().replace(ReplicationState::Replicating);

        /*  ####################        Replicating         ######################  */
        self.replicate(tables)
    }

    fn drop_replication_slot(&self, client: Arc<RefCell<Client>>) {
        let slot = self.details.slot_name.clone();
        let res = client
            .borrow_mut()
            .simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref());
        match res {
            Ok(_) => debug!("dropped replication slot {}", slot),
            Err(_) => debug!("failed to drop replication slot..."),
        };
    }

    fn create_replication_slot(
        &self,
        client: Arc<RefCell<Client>>,
    ) -> Result<Option<String>, ConnectorError> {
        let details = Arc::clone(&self.details);

        let create_replication_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            details.slot_name
        );

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&create_replication_slot_query)
            .map_err(|_e| {
                let slot_name = self.details.slot_name.clone();
                debug!("failed to create replication slot {}", slot_name);
                ConnectorError::PostgresConnectorError(PostgresConnectorError::CreateSlotError(
                    slot_name,
                ))
            })?;

        if let SimpleQueryMessage::Row(row) = &slot_query_row[0] {
            Ok(row.get("consistent_point").map(|lsn| lsn.to_string()))
        } else {
            Err(UnexpectedQueryMessageError)
        }
    }

    fn replication_slot_exists(
        &self,
        client: Arc<RefCell<Client>>,
    ) -> Result<bool, ConnectorError> {
        let details = Arc::clone(&self.details);

        let replication_slot_info_query = format!(
            r#"SELECT * FROM pg_replication_slots where slot_name = '{}';"#,
            details.slot_name
        );

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&replication_slot_info_query)
            .map_err(|_e| {
                debug!("failed to begin txn for replication");
                ConnectorError::PostgresConnectorError(PostgresConnectorError::FetchReplicationSlot)
            })?;

        Ok(!slot_query_row.is_empty())
    }

    fn replicate(&self, tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        let rt = Runtime::new().unwrap();
        let ingestor = self.ingestor.clone();
        let lsn = self.lsn.borrow();
        let lsn = lsn
            .as_ref()
            .map_or(Err(LSNNotStoredError), |x| Ok(x.to_string()))?;

        let publication_name = self.details.publication_name.clone();
        let slot_name = self.details.slot_name.clone();
        rt.block_on(async {
            let mut replicator = CDCHandler {
                replication_conn_config: self.details.replication_conn_config.clone(),
                ingestor,
                lsn,
                publication_name,
                slot_name,
                last_commit_lsn: 0,
                connector_id: self.connector_id,
            };
            replicator.start(tables).await
        })
    }
}
