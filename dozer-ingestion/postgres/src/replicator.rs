use dozer_ingestion_connector::dozer_types::bytes;
use dozer_ingestion_connector::dozer_types::chrono::{TimeZone, Utc};
use dozer_ingestion_connector::dozer_types::log::{error, info};
use dozer_ingestion_connector::dozer_types::models::ingestion_types::{
    IngestionMessage, TransactionInfo,
};
use dozer_ingestion_connector::dozer_types::node::OpIdentifier;
use dozer_ingestion_connector::futures::StreamExt;
use dozer_ingestion_connector::Ingestor;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};
use postgres_protocol::Lsn;
use postgres_types::PgLsn;
use tokio_postgres::Error;

use std::pin::Pin;
use std::time::SystemTime;

use crate::connection::client::Client;
use crate::connection::helper::{self, is_network_failure};
use crate::xlog_mapper::XlogMapper;
use crate::PostgresConnectorError;

use super::schema::helper::PostgresTableInfo;
use super::xlog_mapper::MappedReplicationMessage;

pub struct CDCHandler<'a> {
    pub name: String,
    pub ingestor: &'a Ingestor,

    pub replication_conn_config: tokio_postgres::Config,
    pub publication_name: String,
    pub slot_name: String,

    pub start_lsn: PgLsn,
    pub begin_lsn: Lsn,
    pub offset_lsn: Lsn,
    pub last_commit_lsn: Lsn,
}

impl<'a> CDCHandler<'a> {
    pub async fn start(
        &mut self,
        tables: Vec<PostgresTableInfo>,
    ) -> Result<(), PostgresConnectorError> {
        let replication_conn_config = self.replication_conn_config.clone();
        let client = helper::connect(replication_conn_config).await?;

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

        self.offset_lsn = Lsn::from(lsn);
        self.last_commit_lsn = Lsn::from(lsn);

        let mut stream =
            LogicalReplicationStream::new(client, self.slot_name.clone(), lsn, options)
                .await
                .map_err(PostgresConnectorError::ReplicationStreamError)?;

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
    ) -> Result<(), PostgresConnectorError> {
        match message {
            Some(Ok(XLogData(body))) => {
                let lsn = body.wal_start();
                let message = mapper.handle_message(body)?;

                match message {
                    Some(MappedReplicationMessage::Commit(lsn)) => {
                        self.last_commit_lsn = lsn;
                        if self
                            .ingestor
                            .handle_message(IngestionMessage::TransactionInfo(
                                TransactionInfo::Commit {
                                    id: Some(OpIdentifier::new(self.begin_lsn, 0)),
                                },
                            ))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    Some(MappedReplicationMessage::Begin) => {
                        self.begin_lsn = lsn;
                    }
                    Some(MappedReplicationMessage::Operation { table_index, op }) => {
                        if self.begin_lsn != self.offset_lsn
                            && self
                                .ingestor
                                .handle_message(IngestionMessage::OperationEvent {
                                    table_index,
                                    op,
                                    id: Some(OpIdentifier::new(self.begin_lsn, 0)),
                                })
                                .await
                                .is_err()
                        {
                            // If the ingestion channel is closed, we should stop the replication
                            return Ok(());
                        }
                    }
                    None => {}
                }

                Ok(())
            }
            Some(Ok(msg)) => {
                error!("Unexpected message: {:?}", msg);
                Err(PostgresConnectorError::UnexpectedReplicationMessageError)
            }
            Some(Err(e)) => Err(PostgresConnectorError::ReplicationStreamError(e)),
            None => Err(PostgresConnectorError::ReplicationStreamEndError),
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
