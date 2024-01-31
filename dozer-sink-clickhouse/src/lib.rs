mod ddl;
mod schema;
#[cfg(test)]
mod tests;

use crate::ClickhouseSinkError::{
    PrimaryKeyNotFound, SchemaFieldNotFoundByIndex, UnsupportedOperation,
};
use clickhouse::inserter::Inserter;
use clickhouse::Client;
use dozer_core::epoch::Epoch;
use dozer_core::node::{PortHandle, Sink, SinkFactory};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_log::storage::Queue;
use dozer_log::tokio::runtime::Runtime;
use dozer_recordstore::ProcessorRecordStore;
use dozer_types::errors::internal::BoxedError;
use dozer_types::log::debug;
use dozer_types::models::endpoint::ClickhouseSinkConfig;

use dozer_types::serde::Serialize;
use dozer_types::tonic::async_trait;
use dozer_types::types::{DozerDuration, DozerPoint, Field, FieldType, Operation, Record, Schema};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::schema::{ClickhouseSchema, ClickhouseTable};
use dozer_types::chrono::{DateTime, FixedOffset, NaiveDate};
use dozer_types::json_types::JsonValue;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::serde_bytes;
use dozer_types::thiserror::{self, Error};

pub const BATCH_SIZE: usize = 100;

#[derive(Error, Debug)]
enum ClickhouseSinkError {
    #[error("Only MergeTree engine is supported for delete operation")]
    UnsupportedOperation,

    #[error("Column {0} not found in sink table")]
    ColumnNotFound(String),

    #[error("Column {0} has type {1} in dozer schema but type {2} in sink table")]
    ColumnTypeMismatch(String, String, String),

    #[error("Clickhouse query error: {0}")]
    ClickhouseQueryError(#[from] clickhouse::error::Error),

    #[error("Primary key not found")]
    PrimaryKeyNotFound,

    #[error("Type {1} is not supported for column {0}")]
    TypeNotSupported(String, String),

    #[error("Sink table does not exist and create_table_options is not set")]
    SinkTableDoesNotExist,

    #[error("Expected primary key {0:?} but got {1:?}")]
    PrimaryKeyMismatch(Vec<String>, Vec<String>),

    #[error("Schema field not found by index {0}")]
    SchemaFieldNotFoundByIndex(usize),
}

#[derive(Debug)]
pub struct ClickhouseSinkFactory {
    runtime: Arc<Runtime>,
    config: ClickhouseSinkConfig,
}

#[derive(Debug, Serialize)]
#[serde(crate = "dozer_types::serde", untagged)]
pub enum FieldWrapper {
    UInt(u64),
    U128(u128),
    Int(i64),
    I128(i128),
    Float(#[cfg_attr(feature= "arbitrary", arbitrary(with = arbitrary_float))] OrderedFloat<f64>),
    Boolean(bool),
    String(String),
    Text(String),
    Binary(#[serde(with = "serde_bytes")] Vec<u8>),
    Decimal(Decimal),
    Timestamp(DateTime<FixedOffset>),
    Date(NaiveDate),
    Json(#[cfg_attr(feature= "arbitrary", arbitrary(with = arb_json::arbitrary_json))] JsonValue),
    Point(DozerPoint),
    Duration(DozerDuration),
    OptionalUInt(Option<u64>),
    OptionalU128(Option<u128>),
    OptionalInt(Option<i64>),
    OptionalI128(Option<i128>),
    OptionalFloat(
        #[cfg_attr(feature= "arbitrary", arbitrary(with = arbitrary_float))]
        Option<OrderedFloat<f64>>,
    ),
    OptionalBoolean(Option<bool>),
    OptionalString(Option<String>),
    OptionalText(Option<String>),
    OptionalDecimal(Option<Decimal>),
    OptionalTimestamp(Option<DateTime<FixedOffset>>),
    OptionalDate(Option<NaiveDate>),
    OptionalJson(
        #[cfg_attr(feature= "arbitrary", arbitrary(with = arb_json::arbitrary_json))]
        Option<JsonValue>,
    ),
    OptionalPoint(Option<DozerPoint>),
    OptionalDuration(Option<DozerDuration>),
    Null(Option<()>),
}

fn convert_field_to_ff(field: Field, nullable: bool) -> FieldWrapper {
    if nullable {
        match field {
            Field::UInt(v) => FieldWrapper::OptionalUInt(Some(v)),
            Field::U128(v) => FieldWrapper::OptionalU128(Some(v)),
            Field::Int(v) => FieldWrapper::OptionalInt(Some(v)),
            Field::I128(v) => FieldWrapper::OptionalI128(Some(v)),
            Field::Float(v) => FieldWrapper::OptionalFloat(Some(v)),
            Field::Boolean(v) => FieldWrapper::OptionalBoolean(Some(v)),
            Field::String(v) => FieldWrapper::OptionalString(Some(v)),
            Field::Text(v) => FieldWrapper::OptionalText(Some(v)),
            Field::Binary(v) => FieldWrapper::Binary(v),
            Field::Decimal(v) => FieldWrapper::OptionalDecimal(Some(v)),
            Field::Timestamp(v) => FieldWrapper::OptionalTimestamp(Some(v)),
            Field::Date(v) => FieldWrapper::OptionalDate(Some(v)),
            Field::Json(v) => FieldWrapper::OptionalJson(Some(v)),
            Field::Point(v) => FieldWrapper::OptionalPoint(Some(v)),
            Field::Duration(v) => FieldWrapper::OptionalDuration(Some(v)),
            Field::Null => FieldWrapper::Null(None),
        }
    } else {
        match field {
            Field::UInt(v) => FieldWrapper::UInt(v),
            Field::U128(v) => FieldWrapper::U128(v),
            Field::Int(v) => FieldWrapper::Int(v),
            Field::I128(v) => FieldWrapper::I128(v),
            Field::Float(v) => FieldWrapper::Float(v),
            Field::Boolean(v) => FieldWrapper::Boolean(v),
            Field::String(v) => FieldWrapper::String(v),
            Field::Text(v) => FieldWrapper::Text(v),
            Field::Binary(v) => FieldWrapper::Binary(v),
            Field::Decimal(v) => FieldWrapper::Decimal(v),
            Field::Timestamp(v) => FieldWrapper::Timestamp(v),
            Field::Date(v) => FieldWrapper::Date(v),
            Field::Json(v) => FieldWrapper::Json(v),
            Field::Point(v) => FieldWrapper::Point(v),
            Field::Duration(v) => FieldWrapper::Duration(v),
            Field::Null => FieldWrapper::Null(None),
        }
    }
}
impl ClickhouseSinkFactory {
    pub fn new(config: ClickhouseSinkConfig, runtime: Arc<Runtime>) -> Self {
        Self { config, runtime }
    }
}

#[async_trait]
impl SinkFactory for ClickhouseSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(&self, input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        debug_assert!(input_schemas.len() == 1);
        Ok(())
    }

    async fn build(
        &self,
        mut input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        let schema = input_schemas.remove(&DEFAULT_PORT_HANDLE).unwrap();
        let mut client = Client::default()
            .with_url(self.config.database_url.clone())
            .with_user(self.config.user.clone())
            .with_database(self.config.database.clone());

        if let Some(password) = self.config.password.clone() {
            client = client.with_password(password);
        }

        let table = ClickhouseSchema::get_clickhouse_table(&client, &self.config, &schema).await?;
        let primary_key_field_names =
            ClickhouseSchema::get_primary_keys(&client, &self.config).await?;

        let primary_key_fields_indexes: Result<Vec<usize>, ClickhouseSinkError> =
            primary_key_field_names
                .iter()
                .map(|primary_key| {
                    schema
                        .fields
                        .iter()
                        .position(|field| field.name == *primary_key)
                        .ok_or(PrimaryKeyNotFound)
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
    pub(crate) client: Client,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) schema: Schema,
    pub(crate) inserter: Inserter<FieldWrapper>,
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
        client: Client,
        config: ClickhouseSinkConfig,
        schema: Schema,
        runtime: Arc<Runtime>,
        table: ClickhouseTable,
        primary_key_fields_indexes: Vec<usize>,
    ) -> Self {
        let fields_list = schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<&str>>();

        let inserter = client
            .inserter(&config.sink_table_name, fields_list.as_slice())
            .unwrap()
            .with_max_rows((BATCH_SIZE * schema.fields.len()).try_into().unwrap());

        Self {
            client,
            runtime,
            schema,
            inserter,
            sink_table_name: config.sink_table_name,
            table,
            primary_key_fields_indexes,
        }
    }

    pub fn commit_insert(&mut self) -> Result<(), BoxedError> {
        self.runtime.block_on(async {
            let stats = self.inserter.commit().await?;
            if stats.rows > 0 {
                debug!(
                    "{} bytes, {} rows, {} transactions have been inserted",
                    stats.bytes, stats.rows, stats.transactions,
                );
            }

            Ok::<(), BoxedError>(())
        })
    }

    fn map_fields(&self, record: Record) -> Result<Vec<FieldWrapper>, ClickhouseSinkError> {
        record
            .values
            .into_iter()
            .enumerate()
            .map(|(index, mut field)| match self.schema.fields.get(index) {
                Some(schema_field) => {
                    if schema_field.r#typ == FieldType::Binary && Field::Null == field {
                        field = Field::Binary(vec![]);
                    }

                    Ok(convert_field_to_ff(field.clone(), schema_field.nullable))
                }
                None => Err(SchemaFieldNotFoundByIndex(index)),
            })
            .collect()
    }
}

impl Sink for ClickhouseSink {
    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        self.commit_insert()
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _record_store: &ProcessorRecordStore,
        op: Operation,
    ) -> Result<(), BoxedError> {
        match op {
            Operation::Insert { new } => {
                let values = self.map_fields(new)?;
                self.runtime.block_on(async {
                    for value in values {
                        self.inserter.write(&value)?;
                    }

                    Ok::<(), BoxedError>(())
                })?;

                self.commit_insert()?;
            }
            Operation::Delete { old } => {
                if self.table.engine != "MergeTree" {
                    return Err(BoxedError::from(UnsupportedOperation));
                }
                let mut conditions = vec![];

                for (index, field) in old.values.iter().enumerate() {
                    if *field != Field::Null {
                        let field_name = self.schema.fields.get(index).unwrap().name.clone();
                        conditions.push(format!("{field_name} = ?"));
                    }
                }

                let mut query = self.client.query(&format!(
                    "DELETE FROM {table_name} WHERE {condition}",
                    condition = conditions.join(" AND "),
                    table_name = self.sink_table_name
                ));

                for field in old.values.iter() {
                    if *field != Field::Null {
                        query = query.bind(field);
                    }
                }

                self.runtime.block_on(async {
                    query.execute().await.unwrap();

                    Ok::<(), BoxedError>(())
                })?;
            }
            Operation::Update { new, old } => {
                let mut updates = vec![];

                for (index, field) in new.values.iter().enumerate() {
                    if self.primary_key_fields_indexes.contains(&index) {
                        continue;
                    }
                    let schema_field = self.schema.fields.get(index).unwrap();
                    let field_name = schema_field.name.clone();
                    if *field == Field::Null {
                        updates.push(format!("{field_name} = NULL"));
                    } else {
                        updates.push(format!("{field_name} = ?"));
                    }
                }

                let mut conditions = vec![];

                for (index, field) in old.values.iter().enumerate() {
                    if !self.primary_key_fields_indexes.contains(&index) {
                        continue;
                    }

                    if *field != Field::Null {
                        let field_name = self.schema.fields.get(index).unwrap().name.clone();
                        conditions.push(format!("{field_name} = ?"));
                    }
                }

                let mut query = self.client.query(&format!(
                    "ALTER TABLE {table_name} UPDATE {updates} WHERE {condition}",
                    condition = conditions.join(" AND "),
                    updates = updates.join(", "),
                    table_name = self.sink_table_name
                ));

                for (index, field) in new.values.iter().enumerate() {
                    if *field == Field::Null || self.primary_key_fields_indexes.contains(&index) {
                        continue;
                    }

                    query = query.bind(field);
                }

                for (index, field) in old.values.iter().enumerate() {
                    if !self.primary_key_fields_indexes.contains(&index) {
                        continue;
                    }

                    if *field != Field::Null {
                        query = query.bind(field);
                    }
                }

                self.runtime.block_on(async {
                    query.execute().await.unwrap();

                    Ok::<(), BoxedError>(())
                })?;
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    for f in self.map_fields(record)? {
                        self.runtime.block_on(async {
                            self.inserter.write(&f)?;
                            Ok::<(), BoxedError>(())
                        })?;
                    }
                }

                self.commit_insert()?;
            }
        }

        Ok(())
    }

    fn persist(&mut self, _epoch: &Epoch, _queue: &Queue) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        _connection_name: String,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}
