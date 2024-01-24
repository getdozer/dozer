use std::sync::Arc;

use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        errors::internal::BoxedError,
        models::ingestion_types::{default_bootstrap_path, JavaScriptConfig},
        node::RestartableState,
        types::{FieldDefinition, FieldType, Schema, SourceDefinition},
    },
    tokio::runtime::Runtime,
    CdcType, Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
};
use js_extension::JsExtension;

#[derive(Debug)]
pub struct JavaScriptConnector {
    runtime: Arc<Runtime>,
    config: JavaScriptConfig,
}

#[async_trait]
impl Connector for JavaScriptConnector {
    // We will return one field, named "value", of type Json to
    // give maximum flexibility to the user.
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        vec![(String::from("value"), Some(FieldType::Json))]
    }

    async fn validate_connection(&self) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, BoxedError> {
        Ok(vec![TableIdentifier {
            schema: None,
            name: "json_records".to_string(),
        }])
    }

    async fn validate_tables(&self, _tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        Ok(())
    }

    async fn list_columns(
        &self,
        _tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        Ok(vec![TableInfo {
            schema: None,
            name: "json_records".to_string(),
            column_names: vec!["value".to_string()],
        }])
    }

    async fn get_schemas(
        &self,
        _table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        Ok(vec![Ok(SourceSchema {
            schema: Schema {
                fields: vec![FieldDefinition {
                    name: "value".to_string(),
                    typ: FieldType::Json,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                }],
                primary_index: vec![],
            },
            cdc_type: CdcType::Nothing,
        })])
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        _tables: Vec<TableInfo>,
        _last_checkpoint: Option<RestartableState>,
    ) -> Result<(), BoxedError> {
        let js_path = self
            .config
            .bootstrap_path
            .clone()
            .unwrap_or_else(default_bootstrap_path);
        let ingestor = ingestor.clone();
        let ext = JsExtension::new(self.runtime.clone(), ingestor, js_path)?;
        ext.run().await
    }
}

impl JavaScriptConnector {
    pub fn new(runtime: Arc<Runtime>, config: JavaScriptConfig) -> Self {
        Self { runtime, config }
    }
}

mod js_extension;
