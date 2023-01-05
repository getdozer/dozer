mod cleanup;
mod client;
mod expectation;
mod framework;

use std::path::PathBuf;

use framework::Framework;

#[tokio::test]
async fn run_e2e_tests() {
    // dozer_tracing::init_telemetry(false).unwrap();

    let framework = Framework::new();

    let current_file_relative_to_workspace: PathBuf = file!().into();
    let current_file = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("..")
        .join(current_file_relative_to_workspace)
        .canonicalize()
        .expect("Failed to find absolute path of current file");

    let cases_dir = current_file
        .parent()
        .expect("Current file path doesn't have parent")
        .join("cases");
    // Traverse all subdirectories in the cases directory.
    for case in std::fs::read_dir(cases_dir)
        .expect("Failed to read cases directory")
        .map(|entry| {
            entry
                .expect("Failed to read entry in cases directory")
                .path()
        })
        .filter(|path| path.is_dir())
    {
        framework.run_test_case(case).await;
    }
}
