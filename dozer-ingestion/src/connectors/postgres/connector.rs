use crate::connectors::connector;
use crate::connectors::postgres::schema_helper::SchemaHelper;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use async_trait::async_trait;
use connector::Connector;
use futures::StreamExt;
use postgres::SimpleQueryMessage;
use postgres_protocol::message::backend::ReplicationMessage::*;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use postgres_protocol::message::backend::{LogicalReplicationMessage, XLogDataBody};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::SimpleQueryMessage::Row;
use tokio_postgres::{Client, NoTls};
use dozer_shared::types::TableInfo;
use crate::connectors::storage::RocksStorage;

pub struct PostgresConfig {
    pub name: String,
    pub tables: Option<Vec<String>>,
    pub conn_str: String,
}

pub struct PostgresConnector {
    name: String,
    conn_str: String,
    conn_str_plain: String,
    tables: Option<Vec<String>>,
    client: Option<Client>,
    lsn: Option<String>,
    storage_client: Option<Arc<RocksStorage>>,
}

#[async_trait]
impl Connector<PostgresConfig, tokio_postgres::Client> for PostgresConnector {
    fn new(config: PostgresConfig) -> PostgresConnector {
        let mut conn_str = config.conn_str.to_owned();
        conn_str.push_str(" replication=database");

        PostgresConnector {
            name: config.name,
            conn_str_plain: config.conn_str,
            conn_str,
            tables: config.tables,
            client: None,
            lsn: None,
            storage_client: None,
        }
    }

    async fn initialize(&mut self, storage_client: Arc<RocksStorage>) {
        self.storage_client = Some(storage_client);
        let client = self.connect().await;
        self.client = Some(client);
        self.create_publication().await;
    }

    async fn connect(&mut self) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
                panic!("Connection failed!");
            }
        });
        client
    }

    async fn test_connection(&self) -> Result<(), Error> {
        let (_client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                Result::Err(Error::new(ErrorKind::ConnectionRefused, e.to_string()))
            } else {
                Result::Ok(())
            }
        })
        .await
        .unwrap()
    }

    async fn get_schema(&self) -> Vec<TableInfo> {
        let mut helper = SchemaHelper {
            conn_str: self.conn_str_plain.clone(),
        };
        helper.get_schema().await
    }

    async fn start(&mut self) {
        self._create_slot_and_sync_snapshot().await;
        self._start_replication().await;
    }

    async fn stop(&self) {}
}

impl PostgresConnector {
    fn get_publication_name(&self) -> String {
        format!("dozer_publication_{}", self.name)
    }

    fn get_slot_name(&self) -> String {
        format!("dozer_slot_{}", self.name)
    }

    async fn _run_simple_query(&self, query: &str) -> Vec<SimpleQueryMessage> {
        self.client
            .as_ref()
            .unwrap()
            .simple_query(query)
            .await
            .unwrap()
    }

    async fn create_publication(&self) {
        let publication_name = self.get_publication_name();
        let table_str: String = match self.tables.as_ref() {
            None => "ALL TABLES".to_string(),
            Some(arr) => format!("TABLE {}", arr.join(" ")).to_string(),
        };

        self._run_simple_query(format!("DROP PUBLICATION IF EXISTS {}", publication_name).as_str())
            .await;

        self._run_simple_query(
            format!("CREATE PUBLICATION {} FOR {}", publication_name, table_str).as_str(),
        )
        .await;
    }

    async fn _create_slot_and_sync_snapshot(&mut self) {
        // Begin Transaction
        self._run_simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await;

        // Create a replication slot
        println!("Creating Slot....");
        let lsn = self._create_replication_slot().await;
        self.lsn = lsn;

        println!("Syncing data....");
        self._sync_snapshot().await;

        // Commit the transaction
        self._run_simple_query("COMMIT;").await;
    }

    pub async fn drop_replication_slot(&self) {
        let slot = self.get_slot_name();
        match self
            ._run_simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref())
            .await
        {
            _ => (),
        }
    }

    async fn _create_replication_slot(&self) -> Option<String> {
        let slot = self.get_slot_name();

        let create_replication_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            slot
        );

        let slot_query_row = self._run_simple_query(&create_replication_slot_query).await;

        let lsn = if let Row(row) = &slot_query_row[0] {
            row.get("consistent_point").unwrap()
        } else {
            panic!("unexpected query message");
        };

        println!("lsn: {:?}", lsn);
        Some(lsn.to_string())
    }

    async fn _sync_snapshot(&mut self) {
        let storage_client = self.storage_client.as_ref().unwrap();
        let mut snapshotter = PostgresSnapshotter {
            tables: self.tables.to_owned(),
            conn_str: self.conn_str_plain.to_owned(),
            storage_client: Arc::clone(&storage_client),
        };

        snapshotter.run().await;
    }

    async fn _start_replication(&mut self) {
        let slot = self.get_slot_name();
        let publication_name = self.get_publication_name();
        let lsn = self.lsn.as_ref().unwrap();
        let options = format!(
            r#"("proto_version" '1', "publication_names" '{publication_name}')"#,
            publication_name = publication_name
        );
        let query = format!(
            r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
            slot, lsn, options
        );
        let copy_stream = self
            .client
            .as_ref()
            .unwrap()
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .unwrap();

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut mapper = XlogMapper::new();
        let mut messages_buffer: Vec<XLogDataBody<LogicalReplicationMessage>> = vec![];
        loop {
            match stream.next().await {
                Some(Ok(XLogData(body))) => {
                    let storage_client = self.storage_client.as_ref().unwrap();
                    mapper
                        .handle_message(body, Arc::clone(&storage_client), &mut messages_buffer)
                        .await;
                }
                Some(Ok(PrimaryKeepAlive(ref k))) => {
                    println!("keep alive: {}", k.reply());
                }

                Some(Ok(msg)) => {
                    println!("{:?}", msg);
                    println!("why i am here ?");
                }
                Some(Err(_)) => panic!("unexpected replication stream error"),
                None => panic!("unexpected replication stream end"),
            }
        }
    }
}
