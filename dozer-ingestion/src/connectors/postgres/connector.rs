use crate::connectors::connector;
use crate::connectors::postgres::snapshotter::PostgresSnapshotter;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use async_trait::async_trait;
use connector::Connector;
use dozer_shared::ingestion::{ColumnInfo, TableInfo};
use dozer_storage::storage::RocksStorage;
use futures::StreamExt;
use postgres::SimpleQueryMessage;
use postgres_protocol::message::backend::ReplicationMessage::*;
use std::sync::Arc;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::SimpleQueryMessage::Row;
use tokio_postgres::{Client, NoTls};
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
    storage_client: Arc<RocksStorage>,
}

#[async_trait]
impl Connector<PostgresConfig, tokio_postgres::Client> for PostgresConnector {
    fn new(config: PostgresConfig, storage_client: Arc<RocksStorage>) -> PostgresConnector {
        let mut conn_str = config.conn_str.to_owned();
        conn_str.push_str(" replication=database");

        PostgresConnector {
            name: config.name,
            conn_str_plain: config.conn_str,
            conn_str,
            tables: config.tables,
            client: None,
            lsn: None,
            storage_client,
        }
    }

    async fn initialize(&mut self) {
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

    async fn get_schema(&self) -> Vec<TableInfo> {
        let query = "select genericInfo.table_name, genericInfo.column_name, genericInfo.is_nullable , genericInfo.udt_name, keyInfo.constraint_type is not null as is_primary_key
        FROM
        (SELECT table_schema, table_catalog, table_name, column_name, is_nullable , data_type , numeric_precision , udt_name, character_maximum_length from  information_schema.columns 
         where table_name  in ( select  table_name from information_schema.tables where table_schema = 'public' ORDER BY table_name)
         order by table_name) genericInfo
         
         left join  
         
         (select constraintUsage.table_name , constraintUsage.column_name , tableconstraints.constraint_name, tableConstraints.constraint_type from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE constraintUsage join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tableConstraints on constraintUsage.table_name = tableConstraints.table_name  and  constraintUsage.constraint_name
        =    tableConstraints.constraint_name and tableConstraints.constraint_type = 'PRIMARY KEY') keyInfo
        
       on genericInfo.table_name = keyInfo.table_name and genericInfo.column_name = keyInfo.column_name
       order by genericInfo.table_schema, genericInfo.table_catalog, genericInfo.table_name, genericInfo.column_name";
        let mut table_schema: Vec<TableInfo> = vec![];
        let results = self._run_simple_query(query).await;
        let mut current_table: String = String::from("");
        for row in results {
            if let Row(row) = row {
                let table_name = row.get(0).unwrap().to_string();
                let column_name = row.get(1).unwrap().to_string();
                let is_nullable = row.get(2).unwrap().to_string();
                let udt_name = row.get(3).unwrap().to_string();
                let primary_key = row.get(4).unwrap().to_string().as_str();
                let is_primary_key = true; //matches!(primary_key, "true" | "t" | "1") ;
                let column_info = ColumnInfo {
                    column_name,
                    is_nullable,
                    udt_name,
                    is_primary_key,
                };
                if current_table != table_name {
                    current_table = table_name.clone();
                    let mut column_vec: Vec<ColumnInfo> = Vec::new();
                    column_vec.push(column_info);
                    table_schema.push(TableInfo {
                        table_name: table_name.clone(),
                        columns: column_vec,
                    })
                } else {
                    let mut current_table_info = table_schema.pop().unwrap();
                    current_table_info.columns.push(column_info);
                    table_schema.push(current_table_info);
                }
            }
        }
        table_schema
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
        let mut snapshotter = PostgresSnapshotter {
            tables: self.tables.to_owned(),
            conn_str: self.conn_str_plain.to_owned(),
            storage_client: Arc::clone(&self.storage_client),
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
        loop {
            match stream.next().await {
                Some(Ok(XLogData(body))) => {
                    mapper
                        .handle_message(body, Arc::clone(&self.storage_client))
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
