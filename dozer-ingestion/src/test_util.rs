use dozer_orchestrator::cli::load_config;
use std::panic;
use std::path::PathBuf;
// #[cfg(not(doc))]
// static TESTS_CONFIG_DIR: Dir<'_> = include_dir!("config/tests/local");
// #[cfg(doc)]
// static TESTS_CONFIG_DIR: Dir<'_> = include_dir!("../config/tests/local");
//
// pub fn load_config(file_name: &str) -> &str {
//     TESTS_CONFIG_DIR
//         .get_file(file_name)
//         .unwrap()
//         .contents_utf8()
//         .unwrap()
// }

use dozer_types::models::app_config::Config;

pub fn run_connector_test<T: FnOnce(Config) + panic::UnwindSafe>(db_type: &str, test: T) {
    let dozer_config_path = PathBuf::from(format!("src/tests/cases/{db_type}/dozer-config.yaml"));

    let dozer_config = load_config(dozer_config_path.to_str().unwrap().to_string())
        .unwrap_or_else(|_e| panic!("Cannot read file"));

    let result = panic::catch_unwind(|| {
        test(dozer_config);
    });

    assert!(result.is_ok())
}
