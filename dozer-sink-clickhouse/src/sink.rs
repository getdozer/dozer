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

const BATCH_SIZE: usize = 100;
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

        let client = ClickhouseClient::new(self.config.clone());

        let config = &self.config;
        if self.config.create_table_options.is_some() {
            client
                .create_table(
                    &config.sink_table_name,
                    &schema.fields,
                    self.config.create_table_options.clone(),
                )
                .await?;
        }
        let table = ClickhouseSchema::get_clickhouse_table(client.clone(), &self.config).await?;

        ClickhouseSchema::compare_with_dozer_schema(client.clone(), &schema, &table).await?;

        let sink = ClickhouseSink::new(
            client,
            self.config.clone(),
            schema,
            self.runtime.clone(),
            table,
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
    batch: Vec<Vec<Field>>,
}

impl Debug for ClickhouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickhouseSink")
            .field("sink_table_name", &self.sink_table_name)
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
    ) -> Self {
        Self {
            client,
            runtime,
            schema,
            sink_table_name: config.sink_table_name,
            table,
            batch: Vec::new(),
        }
    }

    fn insert_values(&mut self, values: &[Field]) -> Result<(), BoxedError> {
        // add values to batch instead of inserting immediately
        self.batch.push(values.to_vec());
        Ok(())
    }

    fn commit_batch(&mut self) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            self.client
                .insert_multi(&self.sink_table_name, &self.schema.fields, &self.batch)
                .await?;

            Ok::<(), BoxedError>(())
        })?;
        self.batch.clear();
        Ok(())
    }
}

impl Sink for ClickhouseSink {
    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        self.commit_batch()?;
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

                if self.batch.len() > BATCH_SIZE - 1 {
                    self.commit_batch()?;
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
                self.commit_batch()?;
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
