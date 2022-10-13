use std::cell::RefCell;
use postgres::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use crossbeam::channel::unbounded;
use log::{debug, warn};
use postgres_types::PgLsn;
use dozer_types::types::OperationEvent;
use crate::connectors::connector::TableInfo;
use crate::connectors::ingestor::{ChannelForwarder, Ingestor, IngestorForwarder};
use crate::connectors::postgres::iterator::{Details, ReplicationState};
use crate::connectors::postgres::iterator::handler::PostgresIteratorHandler;
use crate::connectors::seq_no_resolver::SeqNoResolver;
use crate::connectors::storage::RocksStorage;

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
                let (_, message): (usize, u64) = bincode::deserialize(&value.as_slice()).unwrap();
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