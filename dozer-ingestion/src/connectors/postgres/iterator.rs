use crate::connectors::postgres::helper;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use crate::connectors::storage::RocksStorage;
use chrono::{TimeZone, Utc};
use crossbeam::channel::bounded;
use dozer_shared::types::OperationEvent;
use futures::{stream, Stream, StreamExt};
use postgres::fallible_iterator::FallibleIterator;
use postgres::{Error, Row};
use postgres_protocol::message::backend::ReplicationMessage::{self, *};
use postgres_protocol::message::backend::{LogicalReplicationMessage, XLogDataBody};
use postgres_types::PgLsn;
use std::cell::RefCell;
use std::sync::{Arc, RwLock};
use std::task::Poll;
use std::thread;
use std::time::SystemTime;
use tokio_postgres::replication::LogicalReplicationStream;

use postgres::Client;
use postgres::SimpleQueryMessage::Row as SimpleRow;

pub struct Details {
    publication_name: String,
    slot_name: String,
    tables: Option<Vec<String>>,
    conn_str: String,
    conn_str_plain: String,
}
pub struct PostgresIteratorHandler {
    details: Arc<Details>,
    storage_client: Arc<RocksStorage>,
    logical_stream: Arc<RwLock<Option<Arc<LogicalReplicationStream>>>>,
    lsn: RefCell<Option<String>>,
    messages_buffer: Vec<XLogDataBody<LogicalReplicationMessage>>,
    state: RefCell<ReplicationState>,
    sender: crossbeam::channel::Sender<postgres::Row>,
}

pub struct PostgresIterator {
    pub receiver: crossbeam::channel::Receiver<postgres::Row>,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicationState {
    Pending,
    SnapshotInProgress,
    CommitPending,
    ReplicatePending,
    Replicating,
}

impl PostgresIterator {
    pub fn new(
        publication_name: String,
        slot_name: String,
        tables: Option<Vec<String>>,
        conn_str: String,
        conn_str_plain: String,
        storage_client: Arc<RocksStorage>,
    ) -> Self {
        let state = RefCell::new(ReplicationState::Pending);
        let lsn = RefCell::new(None);
        let details = Arc::new(Details {
            publication_name,
            slot_name,
            tables,
            conn_str,
            conn_str_plain,
        });
        let (tx, rx) = bounded::<Row>(100);
        // let (s, r) = bounded(1);
        thread::spawn(move || {
            let mut stream_inner = PostgresIteratorHandler {
                details,
                storage_client,
                state,
                lsn,
                logical_stream: Arc::new(RwLock::new(None)),
                messages_buffer: vec![],
                sender: tx,
            };
            stream_inner._start().unwrap();
        });
        PostgresIterator { receiver: rx }
    }
}

impl Iterator for PostgresIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        println!("in PostgresIterator::loop");
        let msg = self.receiver.recv();
        match msg {
            Ok(msg) => {
                println!("{:?}", msg);
                Some(msg)
            }
            Err(e) => {
                println!("{:?}", e.to_string());
                None
            }
        }
    }
}

impl PostgresIteratorHandler {
    fn _start(&mut self) -> Result<(), Error> {
        let details = Arc::clone(&self.details);
        let conn_str = details.conn_str.to_owned();
        let conn_str_plain = details.conn_str_plain.to_owned();
        let client = Arc::new(RefCell::new(helper::connect(conn_str)?));
        let client_plain = Arc::new(RefCell::new(helper::connect(conn_str_plain)?));
        let storage_client = Arc::clone(&self.storage_client);

        client
            .clone()
            .borrow_mut()
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")?;
        // Create a replication slot
        println!("Creating Slot....");

        self._create_replication_slot(client.clone())?;

        println!("Initializing snapshots...");

        self.state
            .clone()
            .replace(ReplicationState::SnapshotInProgress);

        let tx = self.sender.clone();

        let snapshotter = PostgresSnapshotter {
            tables: details.tables.to_owned(),
            conn_str: details.conn_str_plain.to_owned(),
            storage_client: Arc::clone(&storage_client),
        };

        let tables = snapshotter.get_tables(client_plain.clone())?;
        println!("Initialized with tables: {:?}", tables);
        for table in tables.iter() {
            let query = format!("select * from {}", table);
            let stmt = client_plain.clone().borrow_mut().prepare(&query)?;
            let columns = stmt.columns();
            println!("{:?}", columns);

            let empty_vec: Vec<String> = Vec::new();
            for msg in client_plain
                .clone()
                .borrow_mut()
                .query_raw(&stmt, empty_vec)?
                .iterator()
            {
                // println!("{:?}", msg);
                match msg {
                    Ok(msg) => {
                        let send_res = tx.send(msg);
                        match send_res {
                            Ok(_) => println!("able to send"),
                            Err(e) => println!("{:?}", e.to_string()),
                        }
                    }
                    Err(e) => {
                        println!("{:?}", e);
                        panic!("Something happened");
                    }
                }
            }
        }
        Ok(())
        // ReplicationState::CommitPending => todo!(),
        // ReplicationState::ReplicatePending => todo!(),
        // ReplicationState::Replicating => todo!(),
    }

    fn _create_replication_slot(&self, client: Arc<RefCell<Client>>) -> Result<(), Error> {
        let details = Arc::clone(&self.details);

        let drop_query = format!(r#"DROP_REPLICATION_SLOT {:?}"#, details.slot_name);
        match client.borrow_mut().simple_query(&drop_query) {
            Ok(_) => println!("previous slot dropped"),
            Err(_) => println!("slot doesnt exist. skipping..."),
        };

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
        Ok(())
    }

    async fn _commit_snapshot(&self, client: Arc<Client>) -> Result<(), Error> {
        // client.simple_query("COMMIT;").await?;
        // let mut guard = self.state.write().unwrap();
        // *guard = ReplicationState::Replicating;
        // drop(guard);
        Ok(())
    }

    async fn _start_replication(&self) -> Result<LogicalReplicationStream, Error> {
        let conn_str = Arc::clone(&self.details).conn_str.to_owned();
        let client: tokio_postgres::Client = helper::async_connect(conn_str).await?;

        let lsn = self.lsn.borrow();
        let lsn = lsn.as_ref().unwrap();
        let details = Arc::clone(&self.details);
        let options = format!(
            r#"("proto_version" '1', "publication_names" '{publication_name}')"#,
            publication_name = details.publication_name
        );
        let query = format!(
            r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
            details.slot_name, lsn, options
        );

        let copy_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;

        let stream = LogicalReplicationStream::new(copy_stream);

        Ok(stream)
    }
    async fn handle_message(
        &self,
        message: Option<Result<ReplicationMessage<LogicalReplicationMessage>, Error>>,
        stream: LogicalReplicationStream,
    ) -> Poll<Option<Result<postgres::Row, postgres::Error>>> {
        let mut mapper = XlogMapper::new();
        let mut messages_buffer: Vec<XLogDataBody<LogicalReplicationMessage>> = vec![];
        // match stream.next().await {
        match message {
            Some(Ok(XLogData(body))) => {
                let storage_client = self.storage_client.clone();
                mapper.handle_message(body, storage_client, &mut messages_buffer);
            }
            Some(Ok(PrimaryKeepAlive(ref k))) => {
                println!("keep alive: {}", k.reply());
                if k.reply() == 1 {
                    // Postgres' keep alive feedback function expects time from 2000-01-01 00:00:00
                    let since_the_epoch = SystemTime::now()
                        .duration_since(SystemTime::from(Utc.ymd(2020, 1, 1).and_hms(0, 0, 0)))
                        .unwrap()
                        .as_millis();

                    tokio::pin!(stream);
                    stream
                        // .as_mut()
                        .standby_status_update(
                            PgLsn::from(k.wal_end() + 1),
                            PgLsn::from(k.wal_end() + 1),
                            PgLsn::from(k.wal_end() + 1),
                            since_the_epoch as i64,
                            1,
                        )
                        .await
                        .unwrap();
                }
            }

            Some(Ok(msg)) => {
                println!("{:?}", msg);
                println!("why i am here ?");
            }
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
        }
        Poll::Pending
    }
}
