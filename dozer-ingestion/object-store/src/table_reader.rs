use datafusion::common::DFSchema;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use dozer_ingestion_connector::dozer_types::arrow_types::from_arrow::{
    map_schema_to_dozer, map_value_to_dozer_field,
};
use dozer_ingestion_connector::dozer_types::log::error;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::IngestionMessage;
use dozer_ingestion_connector::dozer_types::types::{Operation, Record};
use dozer_ingestion_connector::futures::StreamExt;
use dozer_ingestion_connector::tokio::sync::mpsc::Sender;
use dozer_ingestion_connector::{tokio, TableInfo};
use std::sync::Arc;

use crate::{ObjectStoreConnectorError, ObjectStoreTableReaderError};

pub async fn read(
    table_index: usize,
    ctx: SessionContext,
    table_path: ListingTableUrl,
    listing_options: ListingOptions,
    table: &TableInfo,
    sender: Sender<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>,
    schema: Option<&DFSchema>,
) -> Result<DFSchema, ObjectStoreConnectorError> {
    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await
        .map_err(ObjectStoreConnectorError::InternalDataFusionError)?;

    let fields = resolved_schema.all_fields();

    let config = ListingTableConfig::new(table_path.clone())
        .with_listing_options(listing_options)
        .with_schema(resolved_schema.clone());

    let provider = Arc::new(
        ListingTable::try_new(config)
            .map_err(ObjectStoreConnectorError::InternalDataFusionError)?,
    );

    let cols: Vec<&str> = if table.column_names.is_empty() {
        fields.iter().map(|f| f.name().as_str()).collect()
    } else {
        table.column_names.iter().map(|c| c.as_str()).collect()
    };
    let dataframe = ctx
        .read_table(provider.clone())
        .map_err(|e| {
            ObjectStoreConnectorError::TableReaderError(
                ObjectStoreTableReaderError::TableReadFailed(e),
            )
        })?
        .select_columns(&cols)
        .map_err(|e| {
            ObjectStoreConnectorError::TableReaderError(
                ObjectStoreTableReaderError::ColumnsSelectFailed(e),
            )
        })?;

    let this_schema = dataframe.schema().to_owned();
    if let Some(schema) = schema {
        if schema != &this_schema {
            return Err(ObjectStoreConnectorError::TableReaderError(
                ObjectStoreTableReaderError::ConflictingSchema(table_path),
            ));
        }
    }
    let data = dataframe.execute_stream().await.map_err(|e| {
        ObjectStoreConnectorError::TableReaderError(
            ObjectStoreTableReaderError::StreamExecutionError(e),
        )
    })?;

    tokio::pin!(data);

    while let Some(batch) = data.next().await {
        let batch = match batch {
            Ok(batch) => batch,
            Err(e) => {
                error!("Error reading record batch from {table_path:?}: {e}");
                continue;
            }
        };

        let batch_schema = batch.schema();
        let dozer_schema = map_schema_to_dozer(&batch_schema)?;

        for row in 0..batch.num_rows() {
            let fields = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(col, column)| {
                    map_value_to_dozer_field(
                        column,
                        row,
                        resolved_schema.field(col).name(),
                        &dozer_schema,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            let evt = Operation::Insert {
                new: Record {
                    values: fields,
                    lifetime: None,
                },
            };

            if sender
                .send(Ok(Some(IngestionMessage::OperationEvent {
                    table_index,
                    op: evt,
                    id: None,
                })))
                .await
                .is_err()
            {
                break;
            }
        }
    }

    // sender.send(Ok(None)).await.unwrap();

    Ok(this_schema.to_owned())
}
