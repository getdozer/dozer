use std::sync::Arc;

use deltalake::datafusion::prelude::SessionContext;
use dozer_ingestion_connector::{
    dozer_types::{
        arrow_types::from_arrow::{map_schema_to_dozer, map_value_to_dozer_field},
        errors::internal::BoxedError,
        models::ingestion_types::{DeltaLakeConfig, IngestionMessage},
        types::{Operation, Record},
    },
    futures::StreamExt,
    tokio,
    utils::TableNotFound,
    Ingestor, TableInfo,
};

pub struct DeltaLakeReader {
    config: DeltaLakeConfig,
}

impl DeltaLakeReader {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }

    pub async fn read(&self, table: &[TableInfo], ingestor: &Ingestor) -> Result<(), BoxedError> {
        for (table_index, table) in table.iter().enumerate() {
            self.read_impl(table_index, table, ingestor).await?;
        }
        Ok(())
    }

    async fn read_impl(
        &self,
        table_index: usize,
        table: &TableInfo,
        ingestor: &Ingestor,
    ) -> Result<(), BoxedError> {
        let table_path = table_path(&self.config, &table.name)?;
        let ctx = SessionContext::new();
        let delta_table = deltalake::open_table(table_path).await?;
        let cols: Vec<&str> = table.column_names.iter().map(|c| c.as_str()).collect();
        let data = ctx
            .read_table(Arc::new(delta_table))?
            .select_columns(&cols)?
            .execute_stream()
            .await?;

        tokio::pin!(data);
        while let Some(Ok(batch)) = data.next().await {
            let batch_schema = batch.schema();
            let dozer_schema = map_schema_to_dozer(&batch_schema)?;
            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| {
                        map_value_to_dozer_field(column, row, cols[col], &dozer_schema).unwrap()
                    })
                    .collect::<Vec<_>>();

                ingestor
                    .handle_message(IngestionMessage::OperationEvent {
                        table_index,
                        op: Operation::Insert {
                            new: Record {
                                values: fields,
                                lifetime: None,
                            },
                        },
                        state: None,
                    })
                    .await
                    .unwrap();
            }
        }
        Ok(())
    }
}

pub fn table_path(config: &DeltaLakeConfig, table_name: &str) -> Result<String, TableNotFound> {
    for delta_table in config.tables.iter() {
        if delta_table.name == table_name {
            return Ok(delta_table.path.clone());
        }
    }
    Err(TableNotFound {
        schema: None,
        name: table_name.to_string(),
    })
}
