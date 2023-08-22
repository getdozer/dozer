use test_suite::{
    run_test_suite_basic_cud, run_test_suite_basic_data_ready, run_test_suite_basic_insert_only,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_local_storage() {
    let _ = env_logger::builder().is_test(true).try_init();

    run_test_suite_basic_data_ready::<test_suite::LocalStorageObjectStoreConnectorTest>().await;
    run_test_suite_basic_insert_only::<test_suite::LocalStorageObjectStoreConnectorTest>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_postgres() {
    let _ = env_logger::builder().is_test(true).try_init();

    run_test_suite_basic_data_ready::<test_suite::PostgresConnectorTest>().await;
    run_test_suite_basic_insert_only::<test_suite::PostgresConnectorTest>().await;
    run_test_suite_basic_cud::<test_suite::PostgresConnectorTest>().await;
}

#[cfg(feature = "mongodb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_mongodb() {
    let _ = env_logger::builder().is_test(true).try_init();

    run_test_suite_basic_data_ready::<test_suite::MongodbConnectorTest>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_nested_dozer() {
    let _ = env_logger::builder().is_test(true).try_init();

    run_test_suite_basic_data_ready::<test_suite::DozerConnectorTest>().await;
    run_test_suite_basic_insert_only::<test_suite::DozerConnectorTest>().await;
    run_test_suite_basic_cud::<test_suite::DozerConnectorTest>().await;
}

mod test_suite;
