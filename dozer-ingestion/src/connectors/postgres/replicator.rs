use crate::connectors::postgres::connection::client::Client;
use crate::connectors::postgres::connection::helper::{self, is_network_failure};
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
use dozer_types::node::OpIdentifier;
use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};
use postgres_types::PgLsn;

use std::pin::Pin;
use std::time::SystemTime;
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
        let client: Client = helper::connect(replication_conn_config).await?;

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

        self.offset_lsn = u64::from(lsn);
        self.last_commit_lsn = u64::from(lsn);

        let mut stream =
            LogicalReplicationStream::new(client, self.slot_name.clone(), lsn, options)
                .await
                .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;

        let tables_columns = tables
            .into_iter()
            .enumerate()
            .map(|(table_index, table_info)| {
                (table_info.relation_id, (table_index, table_info.columns))
            })
            .collect();
        let mut mapper = XlogMapper::new(tables_columns);

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
                                .handle_message(IngestionMessage::OperationEvent {
                                    table_index,
                                    op,
                                    id: Some(OpIdentifier::new(self.begin_lsn, self.seq_no)),
                                })
                                .await
                                .map_err(|_| ConnectorError::IngestorError)?;
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

pub struct LogicalReplicationStream {
    client: Client,
    slot_name: String,
    resume_lsn: PgLsn,
    options: String,
    inner: Pin<Box<tokio_postgres::replication::LogicalReplicationStream>>,
}

impl LogicalReplicationStream {
    pub async fn new(
        mut client: Client,
        slot_name: String,
        lsn: PgLsn,
        options: String,
    ) -> Result<Self, tokio_postgres::Error> {
        let inner =
            Box::pin(Self::open_replication_stream(&mut client, &slot_name, lsn, &options).await?);
        Ok(Self {
            client,
            slot_name,
            resume_lsn: lsn,
            options,
            inner,
        })
    }

    pub async fn next(
        &mut self,
    ) -> Option<Result<ReplicationMessage<LogicalReplicationMessage>, tokio_postgres::Error>> {
        loop {
            let result = self.inner.next().await;
            match result.as_ref() {
                Some(Err(err)) if is_network_failure(err) => {
                    if let Err(err) = self.resume().await {
                        return Some(Err(err));
                    }
                    continue;
                }
                Some(Ok(XLogData(body))) => self.resume_lsn = body.wal_end().into(),
                _ => {}
            }
            return result;
        }
    }

    pub async fn standby_status_update(
        &mut self,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        ts: i64,
        reply: u8,
    ) -> Result<(), Error> {
        loop {
            match self
                .inner
                .as_mut()
                .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, reply)
                .await
            {
                Err(err) if is_network_failure(&err) => {
                    self.resume().await?;
                    continue;
                }
                _ => {}
            }
            break Ok(());
        }
    }

    async fn resume(&mut self) -> Result<(), tokio_postgres::Error> {
        self.client.reconnect().await?;

        let stream = Self::open_replication_stream(
            &mut self.client,
            &self.slot_name,
            self.resume_lsn,
            &self.options,
        )
        .await?;

        self.inner = Box::pin(stream);

        Ok(())
    }

    async fn open_replication_stream(
        client: &mut Client,
        slot_name: &str,
        lsn: PgLsn,
        options: &str,
    ) -> Result<tokio_postgres::replication::LogicalReplicationStream, tokio_postgres::Error> {
        let query = format!(
            r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
            slot_name, lsn, options
        );

        let copy_stream = client.copy_both_simple::<bytes::Bytes>(&query).await?;

        Ok(tokio_postgres::replication::LogicalReplicationStream::new(
            copy_stream,
        ))
    }
}
