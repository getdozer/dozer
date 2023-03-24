use test_suite::{run_test_suite_basic_data_ready, run_test_suite_basic_insert_only};

#[test]
fn test_connectors() {
    let _ = env_logger::builder().is_test(true).try_init();
    run_test_suite_basic_data_ready::<test_suite::LocalStorageObjectStoreConnectorTest>();
    run_test_suite_basic_insert_only::<test_suite::LocalStorageObjectStoreConnectorTest>();
}

mod test_suite;
