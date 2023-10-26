use std::collections::HashMap;

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use dozer_cli::shutdown::{self, ShutdownSender};
use dozer_cli::simple::SimpleOrchestrator;
use dozer_ingestion_connector::dozer_types::{
    grpc_types::{
        conversions::field_to_grpc,
        ingest::{ingest_service_client::IngestServiceClient, IngestRequest, OperationType},
        types::Record,
    },
    log::info,
    models::{
        api_endpoint::ApiEndpoint,
        ingestion_types::{
            GrpcConfig, GrpcConfigSchemas, NestedDozerConfig, NestedDozerLogOptions,
        },
        source::Source,
    },
    serde_json,
    tonic::transport::Channel,
    types::{Field, FieldDefinition, FieldType},
};
use dozer_ingestion_connector::{async_trait, dozer_types, CdcType, SourceSchema};
use dozer_ingestion_dozer::NestedDozerConnector;

use futures::lock::Mutex;
use tempdir::TempDir;
use tokio::runtime::Runtime;

use crate::test_suite::records::Operation;
use crate::test_suite::{
    CudConnectorTest, DataReadyConnectorTest, FieldsAndPk, InsertOnlyConnectorTest,
};

pub struct DozerConnectorTest {
    _tmpdir: TempDir,
    shutdown: Option<(JoinHandle<()>, ShutdownSender)>,
    ingest_state: Arc<Mutex<(u32, IngestServiceClient<Channel>)>>,
    schema: String,
}

async fn ingest(
    client: &mut IngestServiceClient<Channel>,
    seq_no: &mut u32,
    schema: String,
    operation: Operation,
) {
    let (typ, old, new) = match operation {
        Operation::Insert { new } => (OperationType::Insert, None, Some(new)),
        Operation::Update { old, new } => (OperationType::Update, Some(old), Some(new)),
        Operation::Delete { old } => (OperationType::Delete, Some(old), None),
    };

    let old = old.map(|fields| Record {
        version: 0,
        values: fields.into_iter().map(field_to_grpc).collect::<Vec<_>>(),
    });
    let new = new.map(|fields| Record {
        version: 0,
        values: fields.into_iter().map(field_to_grpc).collect::<Vec<_>>(),
    });

    let request = IngestRequest {
        seq_no: *seq_no,
        schema_name: schema.to_owned(),
        typ: typ as _,
        new,
        old,
    };
    *seq_no += 1;
    client
        .ingest(request)
        .await
        .expect("Failed to ingest record");
}

#[async_trait]
impl DataReadyConnectorTest for DozerConnectorTest {
    type Connector = NestedDozerConnector;

    async fn new() -> (Self, Self::Connector) {
        let table_name = "asdf".to_owned();
        let (connector, test) = create_nested_dozer_server(
            table_name.clone(),
            (
                vec![
                    FieldDefinition {
                        name: "id".to_owned(),
                        typ: FieldType::Int,
                        nullable: false,
                        source: Default::default(),
                    },
                    FieldDefinition {
                        name: "name".to_owned(),
                        typ: FieldType::String,
                        nullable: false,
                        source: Default::default(),
                    },
                ],
                vec![0],
            ),
        )
        .await;

        {
            let (seq_no, client) = &mut *test.ingest_state.lock().await;
            ingest(
                client,
                seq_no,
                table_name,
                Operation::Insert {
                    new: vec![Field::Int(6), Field::String("test".to_owned())],
                },
            )
            .await;
        }
        (test, connector)
    }
}

#[async_trait]
impl InsertOnlyConnectorTest for DozerConnectorTest {
    type Connector = NestedDozerConnector;

    async fn new(
        schema_name: Option<String>,
        table_name: String,
        schema: FieldsAndPk,
        records: Vec<Vec<Field>>,
    ) -> Option<(Self, Self::Connector, FieldsAndPk)> {
        // Not supported in gRPC, which we use to feed the nested dozer instance
        if schema_name.is_some() || schema.1.is_empty() {
            return None;
        }

        let (connector, test) =
            create_nested_dozer_server(table_name.clone(), schema.clone()).await;

        {
            let (seq_no, client) = &mut *test.ingest_state.lock().await;
            for record in records {
                ingest(
                    client,
                    seq_no,
                    table_name.clone(),
                    Operation::Insert { new: record },
                )
                .await;
            }
        }

        Some((test, connector, schema))
    }
}

#[async_trait]
impl CudConnectorTest for DozerConnectorTest {
    async fn start_cud(&self, operations: Vec<Operation>) {
        let state = self.ingest_state.clone();
        let schema = self.schema.clone();

        tokio::spawn(async move {
            let (seq_no, client) = &mut *state.lock().await;
            for op in operations {
                ingest(client, seq_no, schema.clone(), op).await;
            }
        });
    }
}

async fn create_nested_dozer_server(
    table_name: String,
    (fields, pk): FieldsAndPk,
) -> (NestedDozerConnector, DozerConnectorTest) {
    let temp_dir = TempDir::new("nested-dozer").expect("Failed to create temp dir");

    let schema = SourceSchema {
        schema: dozer_types::types::Schema {
            fields,
            primary_index: pk,
        },
        cdc_type: CdcType::FullChanges,
    };
    let schemas = HashMap::from([(table_name.clone(), schema)]);
    let schema_string = serde_json::to_string(&schemas).expect("Failed to serialize schema");
    let dozer_config_path = temp_dir.path().join("dozer-config.yaml");
    std::fs::write(&dozer_config_path, DOZER_CONFIG).expect("Failed to write dozer config");

    let grpc_config = GrpcConfig {
        host: Some("0.0.0.0".to_owned()),
        port: Some(8085),
        schemas: GrpcConfigSchemas::Inline(schema_string),
        adapter: Some("default".to_owned()),
    };

    let config = dozer_types::models::config::Config {
        version: 1,
        app_name: "nested-dozer-connector-test".to_owned(),
        home_dir: None,
        cache_dir: None,
        connections: vec![dozer_types::models::connection::Connection {
            config: dozer_types::models::connection::ConnectionConfig::Grpc(grpc_config),
            name: "ingest".to_owned(),
        }],
        sources: vec![Source {
            name: table_name.clone(),
            table_name: table_name.clone(),
            columns: vec![],
            connection: "ingest".to_owned(),
            schema: None,
            refresh_config: Default::default(),
        }],
        endpoints: vec![ApiEndpoint {
            name: table_name.to_owned(),
            path: "/test".to_owned(),
            table_name: table_name.clone(),
            index: Default::default(),
            conflict_resolution: Default::default(),
            version: None,
            log_reader_options: Default::default(),
        }],
        ..Default::default()
    };

    let dozer_runtime = Runtime::new().expect("Failed to start tokio runtime for nested dozer");
    let runtime = Arc::new(dozer_runtime);
    let directory = temp_dir.path().to_owned().try_into().unwrap();
    let mut dozer = SimpleOrchestrator::new(directory, config, runtime.clone(), Default::default());
    let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
    let dozer_thread = std::thread::spawn(move || dozer.run_all(shutdown_receiver, false).unwrap());

    let client = try_connect_ingest("http://localhost:8085".to_owned()).await;

    let connector = NestedDozerConnector::new(NestedDozerConfig {
        url: "http://localhost:50053".to_owned(),
        log_options: NestedDozerLogOptions {
            batch_size: Some(1),
            timeout_in_millis: Some(3000),
            buffer_size: Some(1),
        },
    });

    let test = DozerConnectorTest {
        _tmpdir: temp_dir,
        shutdown: Some((dozer_thread, shutdown_sender)),
        ingest_state: Arc::new(Mutex::new((0, client))),
        schema: table_name,
    };

    (connector, test)
}

async fn try_connect_ingest(addr: String) -> IngestServiceClient<Channel> {
    for _ in 0..20 {
        if let Ok(client) = IngestServiceClient::connect(addr.clone()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    panic!("Could not connect to ingest service");
}
impl Drop for DozerConnectorTest {
    fn drop(&mut self) {
        if let Some((join_handle, shutdown)) = self.shutdown.take() {
            shutdown.shutdown();
            info!("Sent shutdown signal");
            join_handle.join().unwrap();
            info!("Joined dozer thread");
        }
    }
}

static DOZER_CONFIG: &str = r#"
app_name: dozer-nested
version: 1
connections:
  - config: !Grpc
      schemas: !Path ./schema.json
    name: ingest

sources:
  - name: test
    table_name: test
    connection: ingest

api:
    app_grpc:
        port: 50054

endpoints:
  - name: test
    path: /test
    table_name: test
"#;
