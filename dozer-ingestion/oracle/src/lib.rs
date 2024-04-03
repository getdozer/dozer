use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        errors::internal::BoxedError,
        log::info,
        models::ingestion_types::{IngestionMessage, OracleConfig, TransactionInfo},
        node::{OpIdentifier, SourceState},
        types::FieldType,
    },
    tokio, Connector, Ingestor, SourceSchemaResult, TableIdentifier, TableInfo,
};

#[derive(Debug)]
pub struct OracleConnector {
    connection_name: String,
    config: OracleConfig,
    connectors: Option<Connectors>,
}

#[derive(Debug, Clone)]
struct Connectors {
    root_connector: connector::Connector,
    pdb_connector: connector::Connector,
    con_id: Option<u32>,
}

const DEFAULT_BATCH_SIZE: usize = 100_000;

impl OracleConnector {
    pub fn new(connection_name: String, config: OracleConfig) -> Self {
        Self {
            connection_name,
            config,
            connectors: None,
        }
    }

    async fn ensure_connection(
        &mut self,
        force_reconnect: bool,
    ) -> Result<Connectors, connector::Error> {
        if self.connectors.is_none() || force_reconnect {
            let connection_name = self.connection_name.clone();
            let config = self.config.clone();
            let pdb = self.config.pdb.clone();
            self.connectors = Some(
                tokio::task::spawn_blocking(move || {
                    let root_connect_string =
                        format!("{}:{}/{}", config.host, config.port, config.sid);
                    let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
                    let mut root_connector = connector::Connector::new(
                        connection_name.clone(),
                        config.user.clone(),
                        &config.password,
                        &root_connect_string,
                        batch_size,
                        config.replicator,
                    )?;

                    let (pdb_connector, con_id) = if let Some(pdb) = pdb {
                        let pdb_connect_string = format!("{}:{}/{}", config.host, config.port, pdb);
                        let pdb_connector = connector::Connector::new(
                            connection_name,
                            config.user.clone(),
                            &config.password,
                            &pdb_connect_string,
                            batch_size,
                            config.replicator,
                        )?;
                        let con_id = root_connector.get_con_id(&pdb)?;
                        (pdb_connector, Some(con_id))
                    } else {
                        (root_connector.clone(), None)
                    };

                    Ok::<_, connector::Error>(Connectors {
                        root_connector,
                        pdb_connector,
                        con_id,
                    })
                })
                .await
                .unwrap()?,
            );
        }
        Ok(self.connectors.as_ref().unwrap().clone())
    }
}

#[async_trait]
impl Connector for OracleConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&mut self) -> Result<(), BoxedError> {
        self.ensure_connection(false).await?;
        Ok(())
    }

    async fn list_tables(&mut self) -> Result<Vec<TableIdentifier>, BoxedError> {
        let mut connectors = self.ensure_connection(false).await?;
        let schemas = self.config.schemas.clone();
        tokio::task::spawn_blocking(move || connectors.pdb_connector.list_tables(&schemas))
            .await
            .unwrap()
            .map_err(Into::into)
    }

    async fn validate_tables(&mut self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        self.list_columns(tables.to_vec()).await?;
        Ok(())
    }

    async fn list_columns(
        &mut self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let mut connectors = self.ensure_connection(false).await?;
        tokio::task::spawn_blocking(move || connectors.pdb_connector.list_columns(tables))
            .await
            .unwrap()
            .map_err(Into::into)
    }

    async fn get_schemas(
        &mut self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let mut connectors = self.ensure_connection(false).await?;
        let table_infos = table_infos.to_vec();
        Ok(
            tokio::task::spawn_blocking(move || connectors.pdb_connector.get_schemas(&table_infos))
                .await
                .unwrap()?
                .into_iter()
                .map(|result| result.map_err(Into::into))
                .collect(),
        )
    }

    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        Ok(vec![])
    }

    async fn start(
        &mut self,
        ingestor: &Ingestor,
        tables: Vec<TableInfo>,
        last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        let checkpoint = if let SourceState::Restartable(last_checkpoint) = last_checkpoint {
            last_checkpoint.txid
        } else {
            info!("No checkpoint passed, starting snapshotting");

            let ingestor_clone = ingestor.clone();
            let tables = tables.clone();
            let mut connectors = self.ensure_connection(false).await?;

            if ingestor
                .handle_message(IngestionMessage::TransactionInfo(
                    TransactionInfo::SnapshottingStarted,
                ))
                .await
                .is_err()
            {
                return Ok(());
            }
            let scn = tokio::task::spawn_blocking(move || {
                connectors.pdb_connector.snapshot(&ingestor_clone, tables)
            })
            .await
            .unwrap()?;
            ingestor
                .handle_message(IngestionMessage::TransactionInfo(
                    TransactionInfo::SnapshottingDone {
                        id: Some(OpIdentifier {
                            txid: scn,
                            seq_in_tx: 0,
                        }),
                    },
                ))
                .await?;
            scn
        };

        info!("Replicating from checkpoint: {}", checkpoint);
        let ingestor = ingestor.clone();
        let schemas = self.get_schemas(&tables).await?;
        let schemas = schemas
            .into_iter()
            .map(|schema| schema.map(|schema| schema.schema))
            .collect::<Result<Vec<_>, _>>()?;
        let mut connectors = self.ensure_connection(false).await?;
        tokio::task::spawn_blocking(move || {
            connectors.root_connector.replicate(
                &ingestor,
                tables,
                schemas,
                checkpoint,
                connectors.con_id,
            )
        })
        .await
        .unwrap();

        Ok(())
    }
}

mod connector;
