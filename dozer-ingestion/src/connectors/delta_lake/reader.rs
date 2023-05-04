use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::TableInfo;
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::arrow_types::from_arrow::map_value_to_dozer_field;
use dozer_types::ingestion_types::{DeltaLakeConfig, IngestionMessage};
use dozer_types::types::{Operation, Record, SchemaIdentifier};
use futures::StreamExt;
use std::sync::Arc;

pub struct DeltaLakeReader {
    config: DeltaLakeConfig,
}

impl DeltaLakeReader {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }

    pub async fn read(&self, table: &[TableInfo], ingestor: &Ingestor) -> ConnectorResult<()> {
        let mut seq_no = 0;
        for (id, table) in table.iter().enumerate() {
            self.read_impl(id as u32, &mut seq_no, table, ingestor)
                .await?;
        }
        Ok(())
    }

    async fn read_impl(
        &self,
        id: u32,
        seq_no: &mut u64,
        table: &TableInfo,
        ingestor: &Ingestor,
    ) -> ConnectorResult<()> {
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
            let metadata = batch_schema.metadata();
            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| {
                        map_value_to_dozer_field(column, &row, cols[col], metadata).unwrap()
                    })
                    .collect::<Vec<_>>();

                ingestor
                    .handle_message(IngestionMessage::new_op(
                        0_u64,
                        *seq_no,
                        Operation::Insert {
                            new: Record {
                                schema_id: Some(SchemaIdentifier { id, version: 0 }),
                                values: fields,
                                lifetime: None,
                            },
                        },
                    ))
                    .unwrap();

                *seq_no += 1;
            }
        }
        Ok(())
    }
}

pub fn table_path(config: &DeltaLakeConfig, table_name: &str) -> ConnectorResult<String> {
    for delta_table in config.tables.iter() {
        if delta_table.name == table_name {
            return Ok(delta_table.path.clone());
        }
    }
    Err(ConnectorError::TableNotFound(format!(
        "Delta table: {table_name} can't find"
    )))
}
