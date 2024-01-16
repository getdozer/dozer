use std::{
    ops::Mul,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use dozer_core::{
    node::{Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_log::tokio;
use dozer_recordstore::ProcessorRecordStore;
use dozer_types::{
    arrow_types::to_arrow::{map_record_to_arrow, map_to_arrow_schema},
    errors::internal::BoxedError,
    log::{debug, error, info},
    models::endpoint::{
        bigquery::{default_batch_size, default_stage_max_size_in_mb, Destination},
        BigQuery as BigQueryConfig,
    },
    rust_decimal::{
        prelude::{FromPrimitive, ToPrimitive},
        Decimal,
    },
    types::{FieldType, Operation, Schema},
};
use gcp_bigquery_client::{
    error::BQError,
    model::{
        dataset::Dataset, job::Job, job_configuration::JobConfiguration,
        job_configuration_load::JobConfigurationLoad, table::Table,
        table_field_schema::TableFieldSchema, table_reference::TableReference,
        table_schema::TableSchema,
    },
    Client,
};
use object_store::{
    gcp::GoogleCloudStorageBuilder, path::Path, BackoffConfig, ObjectStore, RetryConfig,
};
use parquet::arrow::ArrowWriter;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Debug)]
pub struct BigQuerySinkFactory {
    config: BigQueryConfig,
}

impl BigQuerySinkFactory {
    pub fn new(config: BigQueryConfig) -> Self {
        Self { config }
    }
}

impl SinkFactory for BigQuerySinkFactory {
    fn get_input_ports(&self) -> Vec<dozer_core::node::PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn prepare(
        &self,
        _input_schemas: std::collections::HashMap<dozer_core::node::PortHandle, Schema>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn build(
        &self,
        mut input_schemas: std::collections::HashMap<dozer_core::node::PortHandle, Schema>,
    ) -> Result<Box<dyn dozer_core::node::Sink>, BoxedError> {
        let schema = input_schemas.remove(&DEFAULT_PORT_HANDLE).unwrap();
        Ok(Box::new(BigQuerySink::new(self.config.clone(), schema)))
    }
}

#[derive(Debug)]
pub struct BigQuerySink {
    batch: Vec<Operation>,
    max_batch_size: usize,
    sender: Sender<Vec<Operation>>,
}

impl Sink for BigQuerySink {
    fn commit(&mut self, _epoch_details: &dozer_core::epoch::Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: dozer_core::node::PortHandle,
        _record_store: &ProcessorRecordStore,
        op: dozer_types::types::Operation,
    ) -> Result<(), BoxedError> {
        self.process_op(op)
    }

    fn persist(
        &mut self,
        _epoch: &dozer_core::epoch::Epoch,
        _queue: &dozer_log::storage::Queue,
    ) -> Result<(), BoxedError> {
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

impl BigQuerySink {
    pub fn new(mut config: BigQueryConfig, schema: Schema) -> Self {
        let (sender, receiver) = channel(20);
        let options = config.options.get_or_insert_with(Default::default);
        let max_batch_size = *options.batch_size.get_or_insert_with(default_batch_size);
        let _var = options
            .stage_max_size_in_mb
            .get_or_insert_with(default_stage_max_size_in_mb);

        Self::start_client(receiver, config.clone(), schema.clone());

        Self {
            sender,
            batch: Vec::new(),
            max_batch_size,
        }
    }

    fn process_op(&mut self, op: dozer_types::types::Operation) -> Result<(), BoxedError> {
        self.batch.push(op);
        if self.batch.len() >= self.max_batch_size {
            self.send_batch();
        }
        Ok(())
    }

    fn send_batch(&mut self) {
        let mut batch = Vec::new();
        std::mem::swap(&mut batch, &mut self.batch);
        debug!("sending {} ops to bigquery client", batch.len());
        if let Err(err) = self.sender.blocking_send(batch) {
            panic!("bigquery client crashed: {err:?}");
        }
    }

    fn start_client(receiver: Receiver<Vec<Operation>>, config: BigQueryConfig, schema: Schema) {
        thread::spawn({
            move || {
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        if let Err(err) = bigquery_client(receiver, config, schema).await {
                            error!("BigQuery client crashed with error: {err:?}");
                        }
                    })
            }
        });
    }
}

async fn bigquery_client(
    mut records_receiver: Receiver<Vec<Operation>>,
    config: BigQueryConfig,
    schema: Schema,
) -> Result<(), BoxedError> {
    info!("Starting BigQuery Sink");

    let Destination {
        stage_gcs_bucket_name: bucket_name,
        project,
        dataset,
        table,
        ..
    } = &config.destination;

    let options = config.options.as_ref().unwrap();
    let max_batch_size = options.batch_size.unwrap();
    let max_stage_size_in_bytes = options
        .stage_max_size_in_mb
        .unwrap()
        .mul(Decimal::from_i32(1024 * 1024).unwrap())
        .to_i64()
        .unwrap() as usize;

    let service_account_key =
        html_escape::decode_html_entities(&config.auth.service_account_key).to_string();
    let gcp_sa_key = yup_oauth2::parse_service_account_key(&service_account_key)?;

    let client = gcp_bigquery_client::Client::from_service_account_key(gcp_sa_key, false).await?;

    create_table(&config, &client, &schema).await?;

    let stage_object_store = GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket_name)
        .with_service_account_key(service_account_key)
        .with_retry(RetryConfig {
            backoff: BackoffConfig::default(),
            max_retries: usize::max_value(),
            retry_timeout: std::time::Duration::from_secs(u64::MAX),
        })
        .build()?;

    let arrow_schema = Arc::new(map_to_arrow_schema(&schema)?);

    let mut buffer: Vec<u8>;
    let mut writer: ArrowWriter<&mut Vec<u8>>;
    let mut records_processed: usize;

    let mut total_staged_size_bytes = 0usize;
    let mut staged_files_uris = Vec::new();

    let mut total_written_bytes = 0usize;

    let batch_start_time = Instant::now();

    macro_rules! reset_segment {
        () => {
            buffer = Vec::new();
            writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None).unwrap();
            records_processed = 0usize;
        };
    }

    reset_segment!();

    loop {
        let Some(ops) = records_receiver.recv().await else {
            break;
        };

        let mut records = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                Operation::Insert { new } => records.push(new),
                Operation::BatchInsert { new } => records.extend(new),
                Operation::Delete { .. } => todo!(),
                Operation::Update { .. } => todo!(),
            }
        }

        let num_recods = records.len();
        for record in records {
            writer.write(&map_record_to_arrow(record, &schema)?)?;
        }

        records_processed += num_recods;

        if records_processed >= max_batch_size {
            debug!("writing parquet file with {records_processed} records");
            writer.close()?;
            let num_bytes = buffer.len();
            let bytes = Bytes::from(buffer);
            reset_segment!();
            let filename = format!(
                "{project}.{dataset}.{table}-{}.parquet",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            );
            stage_object_store
                .put(&Path::from(filename.clone()), bytes)
                .await?;

            staged_files_uris.push(format!("gs://{bucket_name}/{filename}"));
            total_staged_size_bytes += num_bytes;

            if total_staged_size_bytes >= max_stage_size_in_bytes {
                debug!(
                    "loading {} parquet files into bigquery",
                    staged_files_uris.len()
                );
                let job = Job {
                    configuration: Some(JobConfiguration {
                        job_timeout_ms: Some("300000".to_string()),
                        load: Some(JobConfigurationLoad {
                            create_disposition: Some("CREATE_NEVER".to_string()),
                            destination_table: Some(TableReference::new(project, dataset, table)),
                            source_format: Some("PARQUET".to_string()),
                            source_uris: Some(staged_files_uris),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                staged_files_uris = Vec::new();

                let job = client.job().insert(project, job).await?;

                let job_ref = job.job_reference.expect("job_reference not found");
                debug!("job_ref: {job_ref:?}");

                while client
                    .job()
                    .get_job(project, job_ref.job_id.as_ref().unwrap(), None)
                    .await?
                    .status
                    .expect("job_status not found")
                    .state
                    != Some("DONE".to_string())
                {
                    tokio::time::sleep(Duration::from_secs_f64(0.5)).await;
                }

                debug!(
                    "Inserted {} MB of data into BigQuery",
                    total_staged_size_bytes / 1024 / 1024
                );

                let batch_time = Instant::now() - batch_start_time;
                debug!(
                    "report: Total time: {} hours, {} minutes, {} seconds {} milliseconds\n  rate: {} MB/sec",
                    batch_time.as_secs() / 3600,
                    (batch_time.as_secs() % 3600) / 60,
                    batch_time.as_secs() % 60,
                    batch_time.subsec_millis(),
                    total_written_bytes as f64 / 1024f64 / 1024f64 / batch_time.as_secs_f64()
                );

                total_written_bytes += total_staged_size_bytes;

                total_staged_size_bytes = 0;
            }
        }
    }

    Ok(())
}

async fn create_table(
    config: &BigQueryConfig,
    client: &Client,
    schema: &Schema,
) -> Result<Table, BQError> {
    let Destination {
        project,
        dataset,
        dataset_location,
        table,
        ..
    } = &config.destination;

    let dataset = {
        let result = client.dataset().get(project, dataset).await;
        match result {
            Ok(dataset) => dataset,
            Err(_) => {
                client
                    .dataset()
                    .create(Dataset::new(project, dataset).location(dataset_location))
                    .await?
            }
        }
    };

    client
        .table()
        .delete_if_exists(project, dataset.dataset_id(), table)
        .await;

    dataset
        .create_table(
            client,
            Table::from_dataset(&dataset, table, table_schema_from_dozer_schema(schema)),
        )
        .await
}

fn table_schema_from_dozer_schema(schema: &Schema) -> TableSchema {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let field_name = &field.name;
            let mut field_schema = match field.typ {
                FieldType::UInt => TableFieldSchema::numeric(field_name),
                FieldType::U128 => TableFieldSchema::big_numeric(field_name),
                FieldType::Int => TableFieldSchema::integer(field_name),
                FieldType::I128 => TableFieldSchema::big_numeric(field_name),
                FieldType::Float => TableFieldSchema::float(field_name),
                FieldType::Boolean => TableFieldSchema::bool(field_name),
                FieldType::String => TableFieldSchema::string(field_name),
                FieldType::Text => TableFieldSchema::string(field_name),
                FieldType::Binary => TableFieldSchema::bytes(field_name),
                FieldType::Decimal => TableFieldSchema::numeric(field_name),
                FieldType::Timestamp => TableFieldSchema::timestamp(field_name),
                FieldType::Date => TableFieldSchema::date(field_name),
                FieldType::Json => TableFieldSchema::json(field_name),
                FieldType::Point => TableFieldSchema::geography(field_name),
                FieldType::Duration => TableFieldSchema::date_time(field_name), // todo: switch to interval data type once it's GA
            };
            if field.nullable {
                field_schema.mode = Some("NULLABLE".to_string());
            }
            field_schema
        })
        .collect::<Vec<_>>();
    TableSchema::new(fields)
}
