use crate::connectors::postgres::connection::helper;
use crate::connectors::postgres::xlog_mapper::XlogMapper;
use crate::errors::ConnectorError;
use crate::errors::ConnectorError::PostgresConnectorError;
use crate::errors::PostgresConnectorError::{
    ReplicationStreamEndError, ReplicationStreamError, UnexpectedReplicationMessageError,
};
use crate::ingestion::Ingestor;
use dozer_types::bytes;
use dozer_types::chrono::{TimeZone, Utc};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::{error, info};
use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};
use postgres_types::PgLsn;

use std::time::SystemTime;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::Error;

use super::schema::helper::PostgresTableInfo;
use super::xlog_mapper::MappedReplicationMessage;

pub struct CDCHandler<'a> {
    pub name: String,
    pub ingestor: &'a Ingestor,

    pub replication_conn_config: tokio_postgres::Config,
    pub publication_name: String,
    pub slot_name: String,

    pub start_lsn: PgLsn,
    pub begin_lsn: u64,
    pub offset_lsn: u64,
    pub last_commit_lsn: u64,

    pub offset: u64,
    pub seq_no: u64,
}

impl<'a> CDCHandler<'a> {
    pub async fn start(&mut self, tables: Vec<PostgresTableInfo>) -> Result<(), ConnectorError> {
        let replication_conn_config = self.replication_conn_config.clone();
        let client: tokio_postgres::Client = helper::connect(replication_conn_config).await?;

        info!(
            "[{}] Starting Replication: {:?}, {:?}",
            self.name.clone(),
            self.start_lsn,
            self.publication_name.clone()
        );

        let lsn = self.start_lsn;
        let options = format!(
            r#"("proto_version" '1', "publication_names" '{publication_name}')"#,
            publication_name = self.publication_name
        );
        let query = format!(
            r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
            self.slot_name, lsn, options
        );

        self.offset_lsn = u64::from(lsn);
        self.last_commit_lsn = u64::from(lsn);

        let copy_stream = client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;

        let stream = LogicalReplicationStream::new(copy_stream);
        let tables_columns = tables
            .into_iter()
            .enumerate()
            .map(|(table_index, table_info)| {
                (table_info.relation_id, (table_index, table_info.columns))
            })
            .collect();
        let mut mapper = XlogMapper::new(tables_columns);

        tokio::pin!(stream);
        loop {
            let message = stream.next().await;
            if let Some(Ok(PrimaryKeepAlive(ref k))) = message {
                if k.reply() == 1 {
                    // Postgres' keep alive feedback function expects time from 2000-01-01 00:00:00
                    let since_the_epoch = SystemTime::now()
                        .duration_since(SystemTime::from(
                            Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
                        ))
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
                let lsn = body.wal_start();
                let message = mapper
                    .handle_message(body)
                    .map_err(PostgresConnectorError)?;

                match message {
                    Some(MappedReplicationMessage::Commit(commit)) => {
                        self.last_commit_lsn = commit.txid;
                    }
                    Some(MappedReplicationMessage::Begin) => {
                        self.begin_lsn = lsn;
                        self.seq_no = 0;
                    }
                    Some(MappedReplicationMessage::Operation { table_index, op }) => {
                        self.seq_no += 1;
                        if self.begin_lsn != self.offset_lsn || self.offset < self.seq_no {
                            self.ingestor
                                .handle_message(IngestionMessage::new_op(
                                    self.begin_lsn,
                                    self.seq_no,
                                    table_index,
                                    op,
                                ))
                                .map_err(ConnectorError::IngestorError)?;
                        }
                    }
                    None => {}
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
