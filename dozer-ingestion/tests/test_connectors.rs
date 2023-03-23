use test_suite::run_test_suite_basic;

#[test]
fn test_connectors() {
    let _ = env_logger::builder().is_test(true).try_init();
    run_test_suite_basic::<test_suite::LocalStorageObjectStoreConnectorTest>();
}

mod test_suite;
