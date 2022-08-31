use std::fmt::format;

use async_trait::async_trait;
use futures::StreamExt;
use postgres_protocol::message::backend::LogicalReplicationMessage::{
    Begin, Commit, Delete, Insert, Origin, Relation, Type, Update,
};
use postgres_protocol::message::backend::ReplicationMessage::*;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::SimpleQueryMessage::Row;
use tokio_postgres::{Client, NoTls};

use connector::Connector;

use crate::connectors::connector;

pub struct PostgresConnector {
    name: String,
    conn_str: Option<String>,
    tables: Option<Vec<String>>,
    client: Option<Client>,
    lsn: Option<String>,
}

#[async_trait]
impl Connector for PostgresConnector {
    fn new(name: String, tables: Option<Vec<String>>) -> PostgresConnector {
        PostgresConnector {
            name,
            conn_str: None,
            tables,
            client: None,
            lsn: None,
        }
    }

    async fn connect(&mut self) {
        let (client, connection) = tokio_postgres::connect(&self.conn_str.as_ref().unwrap(), NoTls)
            .await
            .unwrap();

        // Initialize client after connection
        self.client = Some(client);

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
    }

    async fn get_schema(&self) {}

    async fn start(&mut self) {
        self._create_slot_and_sync_snapshot().await;
        self._start_replication().await;
    }

    async fn stop(&self) {}
}

impl PostgresConnector {
    pub async fn initialize(&mut self, conn_str: String) {
        self.conn_str = Some(conn_str);
        self.connect().await;
        self.create_publication().await;
    }

    fn get_publication_name(&self) -> String {
        format!("dozer_publication_{}", self.name)
    }

    fn get_slot_name(&self) -> String {
        format!("dozer_slot_{}", self.name)
    }

    async fn create_publication(&self) {
        let publication_name = self.get_publication_name();
        let table_str: String = match self.tables.as_ref() {
            None => "ALL TABLES".to_string(),
            Some(arr) => format!("TABLE {}", arr.join(" ")).to_string(),
        };

        self.client
            .as_ref()
            .unwrap()
            .simple_query(format!("DROP PUBLICATION IF EXISTS {}", publication_name).as_str())
            .await
            .unwrap();
        self.client
            .as_ref()
            .unwrap()
            .simple_query(
                format!("CREATE PUBLICATION {} FOR {}", publication_name, table_str).as_str(),
            )
            .await
            .unwrap();
    }

    async fn _create_slot_and_sync_snapshot(&mut self) {
        // Begin Transaction
        self.client
            .as_ref()
            .unwrap()
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await
            .unwrap();

        // Create a replication slot
        let lsn = self._create_replication_slot().await;
        self.lsn = lsn;

        self._sync_snapshot().await;

        // Commit the transaction
        self.client
            .as_ref()
            .unwrap()
            .simple_query("COMMIT;")
            .await
            .unwrap();
    }

    pub async fn drop_replication_slot(&self) {
        let slot = self.get_slot_name();
        self.client
            .as_ref()
            .unwrap()
            .simple_query(format!("select pg_drop_replication_slot('{}');", slot).as_ref())
            .await
            .unwrap();
    }
    async fn _create_replication_slot(&self) -> Option<String> {
        let slot = self.get_slot_name();

        let create_replication_slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} LOGICAL "pgoutput" USE_SNAPSHOT"#,
            slot
        );

        let slot_query = self
            .client
            .as_ref()
            .unwrap()
            .simple_query(&create_replication_slot_query)
            .await
            .unwrap();

        let lsn = if let Row(row) = &slot_query[0] {
            row.get("consistent_point").unwrap()
        } else {
            panic!("unexpected query message");
        };

        println!("lsn: {:?}", lsn);
        Some(lsn.to_string())
    }

    async fn _sync_snapshot(&self) {}

    async fn _start_replication(&self) {
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

        loop {
            match stream.next().await {
                Some(Ok(XLogData(body))) => {
                    println!("received message");
                    match body.data() {
                        // Insert(insert) => {
                        //     println!("insert:");
                        //     println!("{:?}", insert.tuple().tuple_data());
                        // }
                        // Update(update) => {
                        //     println!("update:");
                        // }
                        // Delete(delete) => {
                        //     println!("delete:");
                        // }
                        // Begin(begin) => {
                        //     println!("begin:");
                        // }
                        // Commit(commit) => {
                        //     println!("commit:")
                        // }
                        //
                        // Relation(relation) => {
                        //     println!("relation:")
                        // }
                        // Origin(origin) => {
                        //     println!("origin:")
                        // }
                        // Type(typ) => {
                        //     println!("type:")
                        // }
                        _ => {
                            panic!("Why is this happening")
                        }
                    }
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
