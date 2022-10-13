use std::str::FromStr;
use crate::connectors::ingestor::{IngestionMessage, Ingestor};
use crate::connectors::postgres::helper;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use log::{debug, warn};
use postgres::Error;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_types::PgLsn;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio_postgres::replication::LogicalReplicationStream;
use dozer_types::types::Commit;

pub struct CDCHandler {
    pub conn_str: String,
    pub publication_name: String,
    pub slot_name: String,
    pub lsn: String,
    pub ingestor: Arc<Mutex<Ingestor>>,
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
        debug!(
            "Starting Replication: {:?}, {:?}",
            lsn,
            self.publication_name.clone()
        );
        let query = format!(
            r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
            self.slot_name, lsn, options
        );

        let pg_lsn = PgLsn::from_str(lsn.as_str()).unwrap();
        let mut last_commit_lsn: u64 = u64::from(pg_lsn);

        debug!("last_commit_lsn: {:?}", last_commit_lsn);
        // Marking point of replication start
        self.ingestor
            .lock()
            .unwrap()
            .handle_message(IngestionMessage::Commit(Commit {
                seq_no: 0,
                lsn: last_commit_lsn
            }));

        let copy_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;

        let stream = LogicalReplicationStream::new(copy_stream);
        let mut mapper = XlogMapper::new();

        tokio::pin!(stream);
        loop {
            let message = stream.next().await;

            match message {
                Some(Ok(XLogData(body))) => {
                    let message = mapper.handle_message(body);
                    if let Some(ingestion_message) = message {
                        if let &IngestionMessage::Commit(commit) = &ingestion_message {
                            last_commit_lsn = commit.lsn;
                        }

                        self.ingestor
                            .lock()
                            .unwrap()
                            .handle_message(ingestion_message);
                    }
                }
                Some(Ok(PrimaryKeepAlive(ref k))) => {
                    if k.reply() == 1 {
                        // Postgres' keep alive feedback function expects time from 2000-01-01 00:00:00
                        let since_the_epoch = SystemTime::now()
                            .duration_since(SystemTime::from(Utc.ymd(2020, 1, 1).and_hms(0, 0, 0)))
                            .unwrap()
                            .as_millis();
                        stream
                            .as_mut()
                            .standby_status_update(
                                PgLsn::from(last_commit_lsn),
                                PgLsn::from(last_commit_lsn),
                                PgLsn::from(last_commit_lsn),
                                since_the_epoch as i64,
                                1,
                            )
                            .await
                            .unwrap();
                    }
                }

                Some(Ok(msg)) => {
                    debug!("{:?}", msg);
                    debug!("why i am here ?");
                }
                Some(Err(e)) => {
                    warn!("{:?}", e);
                    panic!("unexpected replication stream error")
                },
                None => panic!("unexpected replication stream end"),
            }
        }
    }
}
