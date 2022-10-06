use crate::connectors::connector::TableInfo;
use crate::connectors::ingestor::{ChannelForwarder, Ingestor, IngestorForwarder};
use crate::connectors::postgres::helper;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::connectors::storage::RocksStorage;
use crossbeam::channel::unbounded;

use dozer_types::types::OperationEvent;
use postgres::Error;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use tokio::runtime::Runtime;

use postgres::Client;
use postgres::SimpleQueryMessage::Row as SimpleRow;
use crate::connectors::seq_no_resolver::SeqNoResolver;

use super::replicator::CDCHandler;

pub struct Details {
    publication_name: String,
    slot_name: String,
    tables: Option<Vec<TableInfo>>,
    conn_str: String,
    conn_str_plain: String,
}
pub struct PostgresIteratorHandler {
    details: Arc<Details>,
    lsn: RefCell<Option<String>>,
    state: RefCell<ReplicationState>,
    ingestor: Arc<Mutex<Ingestor>>,
}

pub struct PostgresIterator {
    receiver: RefCell<Option<crossbeam::channel::Receiver<OperationEvent>>>,
    details: Arc<Details>,
    storage_client: Arc<RocksStorage>,
    seq_storage_client: Arc<RocksStorage>,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicationState {
    Pending,
    SnapshotInProgress,
    Replicating,
}

impl PostgresIterator {
    pub fn new(
        publication_name: String,
        slot_name: String,
        tables: Option<Vec<TableInfo>>,
        conn_str: String,
        conn_str_plain: String,
        storage_client: Arc<RocksStorage>,
        seq_storage_client: Arc<RocksStorage>
    ) -> Self {
        let details = Arc::new(Details {
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
            seq_storage_client
        }
    }
}

impl PostgresIterator {
    pub fn start(&self, seq_no_resolver: Arc<Mutex<SeqNoResolver>>) -> Result<JoinHandle<()>, Error> {
        let state = RefCell::new(ReplicationState::Pending);
        let lsn = RefCell::new(None);
        let details = self.details.clone();

        let (tx, rx) = unbounded::<OperationEvent>();

        self.receiver.replace(Some(rx));

        let forwarder: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        // ingestor.initialize(forwarder);
        let storage_client = self.storage_client.clone();
        let seq_storage_client = self.seq_storage_client.clone();
        let ingestor = Arc::new(Mutex::new(Ingestor::new(
            storage_client,
            seq_storage_client,
            forwarder,
            seq_no_resolver
        )));

        Ok(thread::spawn(move || {
            let mut stream_inner = PostgresIteratorHandler {
                details,
                ingestor,
                state,
                lsn,
            };
            stream_inner._start().unwrap();
        }))
    }
}
impl Iterator for PostgresIterator {
    // type Item = Row;
    type Item = OperationEvent;
    fn next(&mut self) -> Option<Self::Item> {
        let msg = self.receiver.borrow().as_ref().unwrap().recv();
        match msg {
            Ok(msg) => {
                // println!("{:?}", msg);
                Some(msg)
            }
            Err(e) => {
                println!("RecvError: {:?}", e.to_string());
                None
            }
        }
    }
}

impl PostgresIteratorHandler {
    /*
     Replication invovles 3 states
        1) Pending
        - Initialite a replication slot.
        - Initialite snapshots

        2) SnapshotInProgress
        - Sync initial snapshots of specified tables
        - Commit with lsn

        3) Replicating
        - Replicate CDC events using lsn
    */
    fn _start(&mut self) -> anyhow::Result<()> {
        let details = Arc::clone(&self.details);
        let conn_str = details.conn_str.to_owned();
        let client = Arc::new(RefCell::new(helper::connect(conn_str)?));

        // let storage_client = Arc::clone(&self.storage_client);

        /*  #####################        Pending             ############################ */
        client
            
            .borrow_mut()
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")?;

        println!("\nCreating Slot....");
        self._create_replication_slot(client.clone())?;

        self.state
            .clone()
            .replace(ReplicationState::SnapshotInProgress);

        /* #####################        SnapshotInProgress         ###################### */
        println!("\nInitializing snapshots...");

        let snapshotter = PostgresSnapshotter {
            tables: details.tables.to_owned(),
            conn_str: details.conn_str_plain.to_owned(),
            ingestor: Arc::clone(&self.ingestor),
        };
        let tables = snapshotter.sync_tables()?;

        println!("\nInitialized with tables: {:?}", tables);

        client.borrow_mut().simple_query("COMMIT;")?;

        self.state.clone().replace(ReplicationState::Replicating);

        /*  ####################        Replicating         ######################  */
        self._replicate()?;

        Ok(())
    }

    fn _create_replication_slot(&self, client: Arc<RefCell<Client>>) -> Result<(), Error> {
        let details = Arc::clone(&self.details);

        let create_replication_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            details.slot_name
        );

        let slot_query_row = client
            .borrow_mut()
            .simple_query(&create_replication_slot_query)?;

        let lsn = if let SimpleRow(row) = &slot_query_row[0] {
            row.get("consistent_point").unwrap()
        } else {
            panic!("unexpected query message");
        };

        println!("lsn: {:?}", lsn);
        self.lsn.replace(Some(lsn.to_string()));

        Ok(())
    }

    fn _replicate(&self) -> Result<(), Error> {
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
