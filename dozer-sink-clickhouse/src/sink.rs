use dozer_core::epoch::Epoch;
use dozer_core::event::EventHub;
use dozer_core::node::{PortHandle, Sink, SinkFactory};
use dozer_core::tokio::runtime::Runtime;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;

use dozer_types::models::sink::ClickhouseSinkConfig;
use dozer_types::node::OpIdentifier;

use crate::client::ClickhouseClient;
use crate::errors::ClickhouseSinkError;
use crate::schema::{ClickhouseSchema, ClickhouseTable};
use dozer_types::tonic::async_trait;
use dozer_types::types::{Field, Operation, Schema, TableOperation};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
pub struct ClickhouseSinkFactory {
    runtime: Arc<Runtime>,
    config: ClickhouseSinkConfig,
}

impl ClickhouseSinkFactory {
    pub fn new(config: ClickhouseSinkConfig, runtime: Arc<Runtime>) -> Self {
        Self { config, runtime }
    }
}

#[async_trait]
impl SinkFactory for ClickhouseSinkFactory {
    fn type_name(&self) -> String {
        "clickhouse".to_string()
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_input_port_name(&self, _port: &PortHandle) -> String {
        self.config.source_table_name.clone()
    }

    fn prepare(&self, input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        debug_assert!(input_schemas.len() == 1);
        Ok(())
    }

    async fn build(
        &self,
        mut input_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        let schema = input_schemas.remove(&DEFAULT_PORT_HANDLE).unwrap();

        let client = ClickhouseClient::root();

        let config = &self.config;
        if self.config.create_table_options.is_some() {
            client
                .create_table(&config.sink_table_name, &schema.fields)
                .await?;
        }
        let table = ClickhouseSchema::get_clickhouse_table(client.clone(), &self.config).await?;

        let primary_key_field_names =
            ClickhouseSchema::get_primary_keys(client.clone(), &self.config).await?;

        let primary_key_fields_indexes: Result<Vec<usize>, ClickhouseSinkError> =
            primary_key_field_names
                .iter()
                .map(|primary_key| {
                    schema
                        .fields
                        .iter()
                        .position(|field| field.name == *primary_key)
                        .ok_or(ClickhouseSinkError::PrimaryKeyNotFound)
                })
                .collect();

        let sink = ClickhouseSink::new(
            client,
            self.config.clone(),
            schema,
            self.runtime.clone(),
            table,
            primary_key_fields_indexes?,
        );

        Ok(Box::new(sink))
    }
}

pub(crate) struct ClickhouseSink {
    pub(crate) client: ClickhouseClient,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) schema: Schema,
    pub(crate) sink_table_name: String,
    pub(crate) table: ClickhouseTable,
    pub(crate) primary_key_fields_indexes: Vec<usize>,
}

impl Debug for ClickhouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickhouseSink")
            .field("sink_table_name", &self.sink_table_name)
            .field(
                "primary_key_fields_indexes",
                &self.primary_key_fields_indexes,
            )
            .field("table", &self.table)
            .field("schema", &self.schema)
            .finish()
    }
}

impl ClickhouseSink {
    pub fn new(
        client: ClickhouseClient,
        config: ClickhouseSinkConfig,
        schema: Schema,
        runtime: Arc<Runtime>,
        table: ClickhouseTable,
        primary_key_fields_indexes: Vec<usize>,
    ) -> Self {
        Self {
            client,
            runtime,
            schema,
            sink_table_name: config.sink_table_name,
            table,
            primary_key_fields_indexes,
        }
    }

    fn insert_values(&self, values: &[Field]) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            self.client
                .insert(&self.sink_table_name, &self.schema.fields, values)
                .await?;
            Ok::<(), BoxedError>(())
        })?;
        Ok(())
    }
}

impl Sink for ClickhouseSink {
    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        match op.op {
            Operation::Insert { new } => {
                if self.table.engine == "CollapsingMergeTree" {
                    let mut values = new.values;
                    values.push(Field::Int(1));

                    self.insert_values(&values)?;
                } else {
                    self.insert_values(&new.values)?;
                }
            }
            Operation::Delete { old } => {
                if self.table.engine != "CollapsingMergeTree" {
                    return Err(BoxedError::from(ClickhouseSinkError::UnsupportedOperation));
                }
                let mut values = old.values;
                values.push(Field::Int(-1));
                self.insert_values(&values)?;
            }
            Operation::Update { new, old } => {
                if self.table.engine != "CollapsingMergeTree" {
                    return Err(BoxedError::from(ClickhouseSinkError::UnsupportedOperation));
                }
                let mut values = old.values;
                values.push(Field::Int(-1));
                self.insert_values(&values)?;

                let mut values = new.values;
                values.push(Field::Int(1));
                self.insert_values(&values)?;
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    let mut values = record.values;
                    values.push(Field::Int(1));
                    self.insert_values(&values)?;
                }
            }
        }

        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        _connection_name: String,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
        _id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn set_source_state(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        Ok(None)
    }
}
