use crate::connectors::postgres::connection_helper;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use crate::errors::ConnectorError;
use crate::errors::ConnectorError::PostgresConnectorError;
use crate::errors::PostgresConnectorError::{
    ReplicationStreamEndError, ReplicationStreamError, UnexpectedReplicationMessageError,
};
use crate::ingestion::Ingestor;
use dozer_types::chrono::{TimeZone, Utc};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::{debug, error};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::Commit;
use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};
use postgres_types::PgLsn;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::Error;

pub struct CDCHandler {
    pub replication_conn_config: tokio_postgres::Config,
    pub publication_name: String,
    pub slot_name: String,
    pub lsn: String,
    pub last_commit_lsn: u64,
    pub ingestor: Arc<RwLock<Ingestor>>,
    pub connector_id: u64,
}

impl CDCHandler {
    pub async fn start(&mut self, running: Arc<AtomicBool>) -> Result<(), ConnectorError> {
        let replication_conn_config = self.replication_conn_config.clone();
        let client: tokio_postgres::Client = connection_helper::async_connect(replication_conn_config).await?;

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
        self.last_commit_lsn = u64::from(pg_lsn);

        debug!("last_commit_lsn: {:?}", self.last_commit_lsn);
        // Marking point of replication start
        self.ingestor
            .write()
            .handle_message((
                self.connector_id,
                IngestionMessage::Commit(Commit {
                    seq_no: 0,
                    lsn: self.last_commit_lsn,
                }),
            ))
            .map_err(ConnectorError::IngestorError)?;

        let copy_stream = client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;

        let stream = LogicalReplicationStream::new(copy_stream);
        let mut mapper = XlogMapper::new();

        tokio::pin!(stream);
        loop {
            if !running.load(Ordering::SeqCst) {
                debug!("Exiting Postgres Replicator on Ctrl-C");
                return Ok(());
            }
            let message = stream.next().await;
            if let Some(Ok(PrimaryKeepAlive(ref k))) = message {
                if k.reply() == 1 {
                    // Postgres' keep alive feedback function expects time from 2000-01-01 00:00:00
                    let since_the_epoch = SystemTime::now()
                        .duration_since(SystemTime::from(Utc.ymd(2020, 1, 1).and_hms(0, 0, 0)))
                        .unwrap()
                        .as_millis();
                    stream
                        .as_mut()
                        .standby_status_update(
                            PgLsn::from(self.last_commit_lsn),
                            PgLsn::from(self.last_commit_lsn),
                            PgLsn::from(self.last_commit_lsn),
                            since_the_epoch as i64,
                            1,
                        )
                        .await
                        .unwrap();
                }
            } else {
                self.handle_replication_message(message, &mut mapper)
                    .await?;
            }
        }
    }

    pub async fn handle_replication_message(
        &mut self,
        message: Option<Result<ReplicationMessage<LogicalReplicationMessage>, Error>>,
        mapper: &mut XlogMapper,
    ) -> Result<(), ConnectorError> {
        match message {
            Some(Ok(XLogData(body))) => {
                let message = mapper.handle_message(body)?;
                if let Some(ingestion_message) = message {
                    if let IngestionMessage::Commit(commit) = ingestion_message {
                        self.last_commit_lsn = commit.lsn;
                    }

                    self.ingestor
                        .write()
                        .handle_message((self.connector_id, ingestion_message))
                        .map_err(ConnectorError::IngestorError)?;
                }
                Ok(())
            }
            Some(Ok(msg)) => {
                error!("Unexpected message: {:?}", msg);
                Err(PostgresConnectorError(UnexpectedReplicationMessageError))
            }
            Some(Err(e)) => Err(PostgresConnectorError(ReplicationStreamError(
                e.to_string(),
            ))),
            None => Err(PostgresConnectorError(ReplicationStreamEndError)),
        }
    }
}
