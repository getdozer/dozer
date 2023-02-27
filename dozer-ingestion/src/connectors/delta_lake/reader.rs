use crate::connectors::delta_lake::ConnectorResult;
use crate::connectors::object_store::map_value_to_dozer_field;
use crate::connectors::{ColumnInfo, TableInfo};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use deltalake::datafusion::prelude::SessionContext;
use dozer_types::ingestion_types::{DeltaLakeConfig, IngestionMessage};
use dozer_types::types::{Operation, Record, SchemaIdentifier};
use futures::StreamExt;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct DeltaLakeReader {
    config: DeltaLakeConfig,
}

impl DeltaLakeReader {
    pub fn new(config: DeltaLakeConfig) -> Self {
        Self { config }
    }

    pub fn read(&self, table: &[TableInfo], ingestor: &Ingestor) -> ConnectorResult<()> {
        for (id, table) in table.iter().enumerate() {
            Runtime::new()
                .unwrap()
                .block_on(self.read_impl(id as u32, table, ingestor))?;
        }
        Ok(())
    }

    async fn read_impl(
        &self,
        id: u32,
        table: &TableInfo,
        ingestor: &Ingestor,
    ) -> ConnectorResult<()> {
        let table_path = table_path(&self.config, &table.table_name)?;
        let ctx = SessionContext::new();
        let delta_table = deltalake::open_table(table_path).await?;
        let table_schema = delta_table.get_schema()?;
        let columns: Vec<ColumnInfo> = match &table.columns {
            Some(columns_list) if !columns_list.is_empty() => columns_list.clone(),
            _ => table_schema
                .get_fields()
                .iter()
                .map(|f| ColumnInfo {
                    name: f.get_name().to_string(),
                    data_type: None,
                })
                .collect(),
        };
        let cols: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        let data = ctx
            .read_table(Arc::new(delta_table))?
            .select_columns(&cols)?
            .execute_stream()
            .await?;

        tokio::pin!(data);
        let mut idx = 0;
        while let Some(Ok(batch)) = data.next().await {
            for row in 0..batch.num_rows() {
                let fields = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(col, column)| map_value_to_dozer_field(column, &row, cols[col]).unwrap())
                    .collect::<Vec<_>>();

                ingestor
                    .handle_message((
                        (0_u64, idx),
                        IngestionMessage::OperationEvent(Operation::Insert {
                            new: Record {
                                schema_id: Some(SchemaIdentifier { id, version: 0 }),
                                values: fields,
                                version: None,
                            },
                        }),
                    ))
                    .unwrap();

                idx += 1;
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
