use std::{sync::Arc, thread};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use dozer_ingestion::test_util::create_test_runtime;
use dozer_types::tonic::transport::Channel;
use dozer_types::{
    arrow::array::{Int32Array, StringArray},
    grpc_types::ingest::{ingest_service_client::IngestServiceClient, IngestArrowRequest},
    indicatif::{MultiProgress, ProgressBar},
    serde_yaml,
};
mod helper;
use crate::helper::TestConfig;
use dozer_types::{
    arrow::{datatypes as arrow_types, record_batch::RecordBatch},
    arrow_types::from_arrow::serialize_record_batch,
};

const ARROW_PORT: u32 = 60056;
const BATCH_SIZE: usize = 100;

fn grpc(criter: &mut Criterion) {
    let runtime = create_test_runtime();
    let configs = load_test_config();

    let multi_pb = MultiProgress::new();
    for config in configs {
        let mut iterator = helper::get_connection_iterator(runtime.clone(), config.clone());
        let pb = helper::get_progress();
        pb.set_message("consumer");

        let pb2 = helper::get_progress();
        pb2.set_message("producer");

        multi_pb.add(pb.clone());
        multi_pb.add(pb2.clone());
        let mut count = 0;

        // Start ingesting using arrow df
        if config.connection.name == "users_arrow" {
            runtime.spawn(ingest_arrow(BATCH_SIZE, config.size, pb2));
        }
        let mut n_count = 0;
        criter.bench_with_input(
            BenchmarkId::new(config.connection.name, config.size),
            &config.size,
            |b, _| {
                b.iter(|| {
                    let r = iterator.next();
                    if r.is_none() {
                        n_count += 1;
                    }
                    count += 1;
                    if count % 100 == 0 {
                        pb.set_position(count as u64);
                    }
                })
            },
        );
    }
}

async fn ingest_arrow(batch_size: usize, total: usize, pb: ProgressBar) {
    let mut idx = 0;
    let schema = arrow_types::Schema::new(vec![
        arrow_types::Field::new("id", arrow_types::DataType::Int32, false),
        arrow_types::Field::new("name", arrow_types::DataType::Utf8, false),
    ]);
    let mut ingest_client = get_grpc_client(ARROW_PORT).await;
    while idx < total - batch_size {
        let o = idx + batch_size;

        let ids = (idx..o).map(|i| i as i32).collect::<Vec<i32>>();
        let names = (idx..o)
            .map(|i| format!("dario_{i}"))
            .collect::<Vec<String>>();
        let a = Int32Array::from_iter(ids);
        let b = StringArray::from_iter_values(names);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a), Arc::new(b)]).unwrap();
        let res = ingest_client
            .ingest_arrow(IngestArrowRequest {
                schema_name: "users".to_string(),
                records: serialize_record_batch(&record_batch),
                seq_no: idx as u32,
                ..Default::default()
            })
            .await;

        if res.is_err() {
            break;
        }
        pb.set_position(idx as u64);
        idx += batch_size;
    }
}

async fn get_grpc_client(port: u32) -> IngestServiceClient<Channel> {
    let retries = 10;
    let url = format!("http://0.0.0.0:{port}");
    let mut res = IngestServiceClient::connect(url.clone()).await;
    for r in 0..retries {
        if res.is_ok() {
            break;
        }
        if r == retries - 1 {
            panic!("failed to connect after {r} times");
        }
        thread::sleep(std::time::Duration::from_millis(300));
        res = IngestServiceClient::connect(url.clone()).await;
    }
    res.unwrap()
}

pub fn load_test_config() -> Vec<TestConfig> {
    let test_config = include_str!("./grpc.yaml");
    serde_yaml::from_str::<Vec<TestConfig>>(test_config).unwrap()
}

criterion_group!(benches, grpc);
criterion_main!(benches);
