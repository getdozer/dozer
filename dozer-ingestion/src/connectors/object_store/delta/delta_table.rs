use deltalake::DeltaOps;
use dozer_types::{
    arrow_types::from_arrow::{map_schema_to_dozer, map_value_to_dozer_field},
    ingestion_types::{DeltaConfig, IngestionMessage},
    types::{Operation, Record, SchemaIdentifier},
};
use futures::StreamExt;
use tokio::sync::mpsc::Sender;
use tonic::async_trait;

use crate::{
    connectors::{
        object_store::{adapters::DozerObjectStore, table_watcher::TableWatcher},
        TableInfo,
    },
    errors::{ConnectorError, ObjectStoreConnectorError},
};

use crate::errors::ObjectStoreConnectorError::SendError;
pub struct DeltaTable<T: DozerObjectStore + Send> {
    id: usize,
    table_config: DeltaConfig,
    store_config: T,
}

impl<T: DozerObjectStore + Send> DeltaTable<T> {
    pub fn new(id: usize, table_config: DeltaConfig, store_config: T) -> Self {
        Self {
            id,
            table_config,
            store_config,
        }
    }
}

#[async_trait]
impl<T: DozerObjectStore + Send> TableWatcher for DeltaTable<T> {
    // async fn watch(
    //     &self,
    //     id: usize,
    //     table: &TableInfo,
    //     config: &impl DozerObjectStore,
    //     ingestor: &Ingestor,
    // ) -> Result<(), ConnectorError> {
    //     let (tx, mut rx) = channel(16);

    //     let params = config.table_params(&table.name)?;

    //     let data = self.snapshot_data(table, config).await?;
    //     tokio::pin!(data);

    //     tx.send(Some(IngestionMessage::new_snapshotting_started(0_u64, 0)))
    //         .await
    //         .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;

    //     let mut seq_no = 1;
    //     while let Some(Ok(batch)) = data.next().await {
    //         let dozer_schema = map_schema_to_dozer(&batch.schema())
    //             .map_err(|e| ConnectorError::InternalError(Box::new(e)))
    //             .unwrap();
    //         for row in 0..batch.num_rows() {
    //             let fields = batch
    //                 .columns()
    //                 .iter()
    //                 .enumerate()
    //                 .map(|(col, column)| {
    //                     map_value_to_dozer_field(
    //                         column,
    //                         &row,
    //                         dozer_schema.fields.get(col).unwrap().name.as_str(),
    //                         &dozer_schema,
    //                     )
    //                     .unwrap()
    //                 })
    //                 .collect::<Vec<_>>();

    //             tx.send(Some(IngestionMessage::new_op(
    //                 0,
    //                 seq_no,
    //                 Operation::Insert {
    //                     new: Record {
    //                         schema_id: Some(SchemaIdentifier {
    //                             id: id as u32,
    //                             version: 0,
    //                         }),
    //                         values: fields,
    //                         lifetime: None,
    //                     },
    //                 },
    //             )))
    //             .await
    //             .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;
    //             seq_no += 1;
    //         }
    //     }

    //     tx.send(Some(IngestionMessage::new_snapshotting_done(0, seq_no)))
    //         .await
    //         .map_err(|e| ConnectorError::InternalError(Box::new(e)))?;
    //     seq_no += 1;

    //     loop {
    //         let maybe_message = rx
    //             .recv()
    //             .await
    //             .ok_or(ConnectorError::ObjectStoreConnectorError(RecvError))?;
    //         match maybe_message {
    //             None => {
    //                 break;
    //             }
    //             Some(message) => {
    //                 ingestor
    //                     .handle_message(message)
    //                     .map_err(ConnectorError::IngestorError)?;
    //             }
    //         }
    //     }
    //     Ok(())
    // }

    async fn snapshot(
        &self,
        id: usize,
        table: &TableInfo,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<u64, ConnectorError> {
        let params = self.store_config.table_params(&table.name)?;

        let delta_table = deltalake::open_table(&params.table_path).await.unwrap();
        let (_, data) = DeltaOps(delta_table).load().await?;

        tokio::pin!(data);

        // self.ingestor
        //     .handle_message(IngestionMessage::new_snapshotting_started(0_u64, 0))
        //     .map_err(ConnectorError::IngestorError)?;

        let mut seq_no = 1;
        while let Some(Ok(batch)) = data.next().await {
            let dozer_schema = map_schema_to_dozer(&batch.schema())
                .map_err(|e| ConnectorError::InternalError(Box::new(e)))
                .unwrap();
            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| {
                        map_value_to_dozer_field(
                            column,
                            &row,
                            dozer_schema.fields.get(col).unwrap().name.as_str(),
                            &dozer_schema,
                        )
                        .unwrap()
                    })
                    .collect::<Vec<_>>();

                let evt = Operation::Insert {
                    new: Record {
                        schema_id: Some(SchemaIdentifier {
                            id: id as u32,
                            version: 0,
                        }),
                        values: fields,
                        lifetime: None,
                    },
                };

                sender.send(Ok(Some(evt))).await.unwrap();

                // self.ingestor
                //     .handle_message(IngestionMessage::new_op(
                //         0,
                //         seq_no,
                //         Operation::Insert {
                //             new: Record {
                //                 schema_id: Some(SchemaIdentifier {
                //                     id: id as u32,
                //                     version: 0,
                //                 }),
                //                 values: fields,
                //                 lifetime: None,
                //             },
                //         },
                //     ))
                //     .map_err(ConnectorError::IngestorError)?;

                seq_no += 1;
            }
        }

        // self.ingestor
        //     .handle_message(IngestionMessage::new_snapshotting_done(0, seq_no))
        //     .map_err(ConnectorError::IngestorError)?;

        seq_no += 1;

        Ok(seq_no)
    }

    async fn ingest(
        &self,
        id: usize,
        table: &TableInfo,
        seq_no: u64,
        sender: Sender<Result<Option<Operation>, ObjectStoreConnectorError>>,
    ) -> Result<u64, ConnectorError> {
        Ok(0)
    }
}
