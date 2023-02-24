use include_dir::{include_dir, Dir};
use std::panic;
use std::path::PathBuf;
#[cfg(not(doc))]
static TESTS_CONFIG_DIR: Dir<'_> = include_dir!("config/tests/local");
#[cfg(doc)]
static TESTS_CONFIG_DIR: Dir<'_> = include_dir!("../config/tests/local");

pub fn load_config(file_name: &str) -> &str {
    TESTS_CONFIG_DIR
        .get_file(file_name)
        .unwrap()
        .contents_utf8()
        .unwrap()
}

use dozer_tests::e2e_tests::run_docker_compose;
use dozer_tests::e2e_tests::running_env;
use dozer_tests::e2e_tests::Case;
use dozer_types::models::app_config::Config;

pub fn run_connector_test<T: FnOnce(Config) + panic::UnwindSafe>(db_type: &str, test: T) {
    let case_dir = PathBuf::from(format!("src/tests/cases/{db_type}"));
    let connections_dir = PathBuf::from("src/tests/connections");
    let case = Case::load_from_case_dir(case_dir, connections_dir);

    // let docker_compose = running_env::create_docker_compose_for_local_runner(&case).unwrap();
    // let container = run_docker_compose(
    //     &docker_compose.path,
    //     &docker_compose.connections_healthy_service_name,
    // );

    let result = panic::catch_unwind(|| {
        test(case.dozer_config);
    });

    // drop(container);

    assert!(result.is_ok())
}
