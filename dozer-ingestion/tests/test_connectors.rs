use std::sync::Arc;

use dozer_ingestion::test_util::create_test_runtime;
use test_suite::{
    run_test_suite_basic_cud, run_test_suite_basic_data_ready, run_test_suite_basic_insert_only,
};
use tokio::runtime::Runtime;

#[test]
fn test_local_storage() {
    let runtime = create_test_runtime();
    runtime.block_on(test_local_storage_impl(runtime.clone()));
}

async fn test_local_storage_impl(runtime: Arc<Runtime>) {
    let _ = env_logger::builder().is_test(true).try_init();

    run_test_suite_basic_data_ready::<test_suite::LocalStorageObjectStoreConnectorTest>(
        runtime.clone(),
    )
    .await;
    run_test_suite_basic_insert_only::<test_suite::LocalStorageObjectStoreConnectorTest>(runtime)
        .await;
}

#[test]
fn test_postgres() {
    let runtime = create_test_runtime();
    runtime.block_on(test_postgres_impl(runtime.clone()));
}

async fn test_postgres_impl(runtime: Arc<Runtime>) {
    let _ = env_logger::builder().is_test(true).try_init();

    run_test_suite_basic_data_ready::<test_suite::PostgresConnectorTest>(runtime.clone()).await;
    run_test_suite_basic_insert_only::<test_suite::PostgresConnectorTest>(runtime.clone()).await;
    run_test_suite_basic_cud::<test_suite::PostgresConnectorTest>(runtime).await;
}

#[cfg(feature = "mongodb")]
#[test]
fn test_mongodb() {
    let runtime = create_test_runtime();
    runtime.block_on(test_mongodb_impl(runtime.clone()));
}

#[cfg(feature = "mongodb")]
async fn test_mongodb_impl(runtime: Arc<Runtime>) {
    let _ = env_logger::builder().is_test(true).try_init();

    run_test_suite_basic_data_ready::<test_suite::MongodbConnectorTest>(runtime).await;
}

mod test_suite;
