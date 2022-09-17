use crate::connectors::ingestor::{Ingestor, IngestorForwarder};
use crate::connectors::postgres::helper;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use postgres::Error;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::{LogicalReplicationMessage, XLogDataBody};
use postgres_types::PgLsn;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_postgres::replication::LogicalReplicationStream;

pub struct CDCHandler {
    pub conn_str: String,
    pub publication_name: String,
    pub slot_name: String,
    pub lsn: String,
    pub ingestor: Arc<Ingestor>,
}
impl CDCHandler {
    pub async fn start(&self) -> Result<(), Error> {
        let conn_str = self.conn_str.clone();
        let client: tokio_postgres::Client = helper::async_connect(conn_str).await?;

        let lsn = self.lsn.clone();
        let options = format!(
            r#"("proto_version" '1', "publication_names" '{publication_name}')"#,
            publication_name = self.publication_name
        );
        println!(
            "Starting Replication: {:?}, {:?}",
            lsn,
            self.publication_name.clone()
        );
        let query = format!(
            r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
            self.slot_name, lsn, options
        );

        let copy_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;

        let stream = LogicalReplicationStream::new(copy_stream);

        tokio::pin!(stream);
        loop {
            let message = stream.next().await;
            let mut mapper = XlogMapper::new();
            let mut messages_buffer: Vec<XLogDataBody<LogicalReplicationMessage>> = vec![];

            match message {
                Some(Ok(XLogData(body))) => {
                    mapper.handle_message(body, self.ingestor.clone(), &mut messages_buffer);
                }
                Some(Ok(PrimaryKeepAlive(ref k))) => {
                    // println!("keep alive: {}", k.reply());
                    if k.reply() == 1 {
                        // Postgres' keep alive feedback function expects time from 2000-01-01 00:00:00
                        let since_the_epoch = SystemTime::now()
                            .duration_since(SystemTime::from(Utc.ymd(2020, 1, 1).and_hms(0, 0, 0)))
                            .unwrap()
                            .as_millis();
                        stream
                            .as_mut()
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
        }
    }
}
