use std::cell::RefCell;
use postgres::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use crossbeam::channel::unbounded;
use log::{debug, error, warn};
use postgres_types::PgLsn;
use dozer_types::types::OperationEvent;
use crate::connectors::connector::TableInfo;
use crate::connectors::ingestor::{ChannelForwarder, Ingestor, IngestorForwarder};
use crate::connectors::seq_no_resolver::SeqNoResolver;
use crate::connectors::storage::RocksStorage;

use anyhow::{bail, Context};
use postgres::Client;
use tokio::runtime::Runtime;
use crate::connectors::postgres::helper;
use crate::connectors::postgres::replicator::CDCHandler;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use tokio_postgres::SimpleQueryMessage;

pub struct Details {
    id: u64,
    publication_name: String,
    slot_name: String,
    tables: Option<Vec<TableInfo>>,
    conn_str: String,
    conn_str_plain: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicationState {
    Pending,
    SnapshotInProgress,
    Replicating,
}

pub struct PostgresIterator {
    receiver: RefCell<Option<crossbeam::channel::Receiver<OperationEvent>>>,
    details: Arc<Details>,
    storage_client: Arc<RocksStorage>,
}

impl PostgresIterator {
    pub fn new(
        id: u64,
        publication_name: String,
        slot_name: String,
        tables: Option<Vec<TableInfo>>,
        conn_str: String,
        conn_str_plain: String,
        storage_client: Arc<RocksStorage>,
    ) -> Self {
        let details = Arc::new(Details {
            id,
            publication_name,
            slot_name,
            tables,
            conn_str,
            conn_str_plain,
        });
        PostgresIterator {
            receiver: RefCell::new(None),
            details,
            storage_client,
        }
    }
}

impl PostgresIterator {
    pub fn start(&self, seq_no_resolver: Arc<Mutex<SeqNoResolver>>) -> Result<JoinHandle<()>, Error> {
        let lsn = RefCell::new(self.get_last_lsn_for_connection());
        let state = RefCell::new(ReplicationState::Pending);
        let details = self.details.clone();

        let (tx, rx) = unbounded::<OperationEvent>();

        self.receiver.replace(Some(rx));

        let forwarder: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        let storage_client = self.storage_client.clone();
        let ingestor = Arc::new(Mutex::new(Ingestor::new(
            storage_client,
            forwarder,
            seq_no_resolver
        )));

        Ok(thread::spawn(move || {
            let mut stream_inner = PostgresIteratorHandler {
                details,
                ingestor,
                state,
                lsn
            };
            stream_inner._start().unwrap();
        }))
    }

    pub fn get_last_lsn_for_connection(&self) -> Option<String> {
        let commit_key = self.storage_client.get_commit_message_key(&(self.details.id as usize));
        let commit_message = self.storage_client.get_db().get(commit_key);
        match commit_message {
            Ok(Some(value)) => {
                let (_, message): (usize, u64) = bincode::deserialize(value.as_slice()).unwrap();
                if message == 0 {
                    None
                } else {
                    debug!("lsn: {:?}", PgLsn::from(message).to_string());
                    Some(PgLsn::from(message).to_string())
                }
            }
            _ => None
        }
    }
}


impl Iterator for PostgresIterator {
    type Item = OperationEvent;
    fn next(&mut self) -> Option<Self::Item> {
        let msg = self.receiver.borrow().as_ref().unwrap().recv();
        match msg {
            Ok(msg) => {
                Some(msg)
            }
            Err(e) => {
                warn!("RecvError: {:?}", e.to_string());
                None
            }
        }
    }
}


pub struct PostgresIteratorHandler {
    pub details: Arc<Details>,
    pub lsn: RefCell<Option<String>>,
    pub state: RefCell<ReplicationState>,
    pub ingestor: Arc<Mutex<Ingestor>>
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
    pub fn _start(&mut self) -> anyhow::Result<()> {
        let details = Arc::clone(&self.details);
        let conn_str = details.conn_str.to_owned();
        let client = Arc::new(RefCell::new(helper::connect(conn_str)?));

        // TODO: Handle cases:
        // - When snapshot replication is not completed
        // - When there is gap between available lsn (in case when slot dropped and new created) and last lsn
        // - When publication tables changes
        if self.lsn.clone().into_inner().is_none() {
            debug!("\nCreating Slot....");
            if let Ok(true) = self.replication_exist(client.clone()) {
                // We dont have lsn, so we need to drop replication slot and start from scratch
                self.drop_replication_slot(client.clone()).context("Replication slot drop failed")?;
            }

            client
                .borrow_mut()
                .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")?;

            let replication_slot_lsn = self.create_replication_slot(client.clone())
                .context("Failed to create replication slot")?;

            self.lsn.replace(replication_slot_lsn);
            self.state.clone().replace(ReplicationState::SnapshotInProgress);

            /* #####################        SnapshotInProgress         ###################### */
            debug!("\nInitializing snapshots...");

            let snapshotter = PostgresSnapshotter {
                tables: details.tables.to_owned(),
                conn_str: details.conn_str_plain.to_owned(),
                ingestor: Arc::clone(&self.ingestor),
            };
            let tables = snapshotter.sync_tables()?;

            debug!("\nInitialized with tables: {:?}", tables);

            client.borrow_mut().simple_query("COMMIT;")?;
        }

        self.state.clone().replace(ReplicationState::Replicating);

        /*  ####################        Replicating         ######################  */
        self.replicate()?;

        Ok(())
    }

    fn drop_replication_slot(&self, client: Arc<RefCell<Client>>) -> anyhow::Result<()> {
        let slot = self.details.slot_name.clone();
        let res =
            client
                .borrow_mut()
                .simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref());
        match res {
            Ok(_) => debug!("dropped replication slot {}", slot),
            Err(e) => {
                error!("{}", e);
                bail!("failed to drop replication slot...")
            },
        }
        Ok(())
    }

    fn create_replication_slot(&self, client: Arc<RefCell<Client>>) -> Result<Option<String>, Error> {
        let details = Arc::clone(&self.details);

        let create_replication_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            details.slot_name
        );

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&create_replication_slot_query)?;

        let lsn = if let SimpleQueryMessage::Row(row) = &slot_query_row[0] {
            row.get("consistent_point").unwrap()
        } else {
            panic!("unexpected query message");
        };

        Ok(Option::from(lsn.to_string()))
    }

    fn replication_exist(&self, client: Arc<RefCell<Client>>) -> anyhow::Result<bool> {
        let details = Arc::clone(&self.details);

        let replication_slot_info_query = format!(
            r#"SELECT * FROM pg_replication_slots where slot_name = '{}';"#,
            details.slot_name
        );

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&replication_slot_info_query)
            .context("fetch of replication slot info failed")?;

        if let SimpleQueryMessage::Row(_row) = &slot_query_row[0] {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn replicate(&self) -> Result<(), Error> {
        let rt = Runtime::new().unwrap();
        let ingestor = self.ingestor.clone();
        let lsn = self.lsn.borrow();
        let lsn = match lsn.as_ref() {
            Some(x) => x.to_string(),
            None => panic!("lsn not stored..."),
        };
        let publication_name = self.details.publication_name.clone();
        let slot_name = self.details.slot_name.clone();
        rt.block_on(async {
            let replicator = CDCHandler {
                conn_str: self.details.conn_str.clone(),
                ingestor,
                lsn,
                publication_name,
                slot_name,
            };
            replicator.start().await
        })
    }
}