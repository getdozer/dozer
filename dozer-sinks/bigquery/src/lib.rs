use std::{
    ops::{Deref, Mul},
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
        self.send_batch();
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
        let (sender, receiver) = channel(1000000);
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
        // debug!("sending {} ops to bigquery client", batch.len());
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

struct Metrics {
    start_time: Instant,
    total_processed_bytes: usize,
    total_parquet_conversion_time: Duration,
    total_parquet_upload_time: Duration,
    total_bigquery_load_time: Duration,
}

impl Metrics {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_processed_bytes: Default::default(),
            total_parquet_conversion_time: Default::default(),
            total_parquet_upload_time: Default::default(),
            total_bigquery_load_time: Default::default(),
        }
    }
}

async fn bigquery_client(
    mut records_receiver: Receiver<Vec<Operation>>,
    config: BigQueryConfig,
    schema: Schema,
) -> Result<(), BoxedError> {
    info!("Starting BigQuery Sink");

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

    let metrics = Arc::new(std::sync::Mutex::new(Metrics::new()));

    let (parquet_sender, parquet_receiver) = channel(1000);
    tokio::spawn(parquet_loader(
        parquet_receiver,
        gcp_sa_key.clone(),
        max_stage_size_in_bytes,
        config.destination.clone(),
        metrics.clone(),
    ));

    let client = gcp_bigquery_client::Client::from_service_account_key(gcp_sa_key, false).await?;

    create_table(&config, &client, &schema).await?;

    let arrow_schema = Arc::new(map_to_arrow_schema(&schema)?);

    let mut records = Vec::new();

    'main: loop {
        loop {
            let capacity = 100;
            let mut results = Vec::with_capacity(capacity);
            let num_results = records_receiver.recv_many(&mut results, capacity).await;
            if num_results == 0 {
                break 'main;
            }
            let continue_reading = num_results < capacity;
            for ops in results {
                // debug!("got {} ops", ops.len());

                for op in ops {
                    match op {
                        Operation::Insert { new } => records.push(new),
                        Operation::BatchInsert { new } => records.extend(new),
                        Operation::Delete { .. } => todo!(),
                        Operation::Update { .. } => todo!(),
                    }
                }
            }
            if !continue_reading || records.len() >= max_batch_size {
                break;
            }
        }

        let num_recods = records.len();

        if num_recods >= max_batch_size {
            debug!("writing parquet file with {num_recods} records");
            let mut parquet_records = Vec::new();
            let schema = schema.clone();
            let arrow_schema = arrow_schema.clone();
            let destination = config.destination.clone();
            let service_account_key = service_account_key.clone();
            let parquet_sender = parquet_sender.clone();
            let metrics = metrics.clone();

            std::mem::swap(&mut parquet_records, &mut records);
            tokio::task::spawn_blocking(move || {
                if let Err(err) = (move || -> Result<(), BoxedError> {
                    let conversion_start_time = Instant::now();

                    let mut buffer = Vec::new();
                    let mut writer =
                        ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None).unwrap();

                    for record in parquet_records {
                        writer.write(&map_record_to_arrow(record, &schema)?)?;
                    }

                    if let Err(err) = writer.close() {
                        error!("parquet error: {err}");
                    }

                    let conversion_time = Instant::now() - conversion_start_time;
                    metrics.lock().unwrap().total_parquet_conversion_time += conversion_time;

                    let Destination {
                        stage_gcs_bucket_name: bucket_name,
                        project,
                        dataset,
                        table,
                        ..
                    } = &destination;

                    let filename = format!(
                        "{project}.{dataset}.{table}-{}.parquet",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                    );

                    let num_bytes = buffer.len();
                    let bytes = Bytes::from(buffer);

                    tokio::spawn({
                        let stage_object_store = GoogleCloudStorageBuilder::new()
                            .with_bucket_name(bucket_name)
                            .with_service_account_key(&service_account_key)
                            .with_retry(RetryConfig {
                                backoff: BackoffConfig::default(),
                                max_retries: usize::max_value(),
                                retry_timeout: std::time::Duration::from_secs(u64::MAX),
                            })
                            .build()?;

                        let sender = parquet_sender;
                        let uri = format!("gs://{bucket_name}/{filename}");
                        async move {
                            let upload_start_time = Instant::now();

                            if let Err(err) = stage_object_store
                                .put(&Path::from(filename.clone()), bytes)
                                .await
                            {
                                error!("failed to upload parquet file {uri}; {err}");
                                return;
                            }

                            let upload_time = Instant::now() - upload_start_time;
                            metrics.lock().unwrap().total_parquet_upload_time += upload_time;

                            let _r = sender
                                .send(ParquetMetadata {
                                    gcs_uri: uri,
                                    size_in_bytes: num_bytes,
                                })
                                .await;
                        }
                    });

                    Ok(())
                })() {
                    error!("error writing parquet: {err}");
                    panic!();
                }
            });
        }
    }

    Ok(())
}

struct ParquetMetadata {
    gcs_uri: String,
    size_in_bytes: usize,
}

fn report(metrics: &Metrics) {
    let uptime = Instant::now() - metrics.start_time;

    struct Format<'a>(&'a Duration);
    impl std::fmt::Display for Format<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let time = self.0;
            let hours = time.as_secs() / 3600;
            let minutes = (time.as_secs() % 3600) / 60;
            let seconds = time.as_secs() % 60;
            let millis = time.subsec_millis();
            write!(f, "{hours:02}:{minutes:02}:{seconds:02}")?;
            if millis != 0 {
                write!(f, ".{millis:03}")?;
            }
            Ok(())
        }
    }

    debug!(
        concat!(
            "\nMetrics Report:\n",
            "  rate: {} MB/sec\n",
            "  uptime: {}\n",
            "  in-memory records-to-parquet conversion time: {}  (parallelized)\n",
            "  parquet upload to GCS time:                   {}  (parallelized)\n",
            "  bigquery load parquet from GCS time:          {}  (parallelized)\n"
        ),
        metrics.total_processed_bytes as f64 / 1024f64 / 1024f64 / uptime.as_secs_f64(),
        Format(&uptime),
        Format(&metrics.total_parquet_conversion_time),
        Format(&metrics.total_parquet_upload_time),
        Format(&metrics.total_bigquery_load_time),
    );
}

async fn parquet_loader(
    mut receiver: Receiver<ParquetMetadata>,
    gcp_sa_key: yup_oauth2::ServiceAccountKey,
    max_stage_size_in_bytes: usize,
    destination: Destination,
    metrics: Arc<std::sync::Mutex<Metrics>>,
) -> Result<(), BoxedError> {
    let mut total_staged_bytes = 0usize;
    let mut staged_files_uris = Vec::new();

    let Destination {
        project,
        dataset,
        table,
        ..
    } = &destination;

    let client = gcp_bigquery_client::Client::from_service_account_key(gcp_sa_key, false).await?;

    loop {
        let Some(ParquetMetadata {
            gcs_uri,
            size_in_bytes,
        }) = receiver.recv().await
        else {
            break;
        };

        total_staged_bytes += size_in_bytes;
        staged_files_uris.push(gcs_uri);

        if total_staged_bytes >= max_stage_size_in_bytes {
            debug!(
                "loading {} parquet files into bigquery",
                staged_files_uris.len()
            );

            let load_start_time = Instant::now();

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
            let job = client.job().insert(project, job).await?;

            let job_ref = job.job_reference.expect("job_reference not found");
            debug!("job_ref: {job_ref:?}");

            metrics.lock().unwrap().total_processed_bytes += total_staged_bytes;
            total_staged_bytes = 0;
            staged_files_uris = Vec::new();

            tokio::spawn({
                let jobs = client.job().clone();
                let project = project.clone();
                let metrics = metrics.clone();
                async move {
                    loop {
                        match jobs
                            .get_job(&project, job_ref.job_id.as_ref().unwrap(), None)
                            .await
                        {
                            Ok(job) => {
                                if job.status.expect("job_status not found").state
                                    == Some("DONE".to_string())
                                {
                                    break;
                                } else {
                                    tokio::time::sleep(Duration::from_secs_f64(0.5)).await;
                                    continue;
                                }
                            }
                            Err(err) => {
                                error!(
                                    "error getting job status for job: {:?}; {err}",
                                    job_ref.job_id
                                );
                                return;
                            }
                        }
                    }

                    let load_time = Instant::now() - load_start_time;
                    metrics.lock().unwrap().total_bigquery_load_time += load_time;

                    report(metrics.lock().unwrap().deref());
                }
            });
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
