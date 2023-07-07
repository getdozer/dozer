use std::process::Command;

use dozer_types::{
    log::{error, info},
    models::app_config::Config,
};
use dozer_utils::Cleanup;

use super::expectation::{ErrorExpectation, Expectation};

mod client;

use client::Client;

/// Dozer API server should be reachable by now.
pub async fn run_test_client(dozer_config: Config, expectations: &[Expectation]) {
    let mut client = Client::new(dozer_config).await;
    for expectation in expectations {
        client.check_expectation(expectation).await;
    }
}

/// Connections should be ready by now. And this function must be given a command that runs dozer.
pub async fn check_error_expectation(
    dozer_command: impl FnMut() -> (Command, Vec<Cleanup>),
    dozer_config_path: &str,
    error_expectation: &ErrorExpectation,
) {
    match error_expectation {
        ErrorExpectation::BuildFailure { message } => {
            check_build_failure(dozer_command, dozer_config_path, message.as_deref());
        }
    }
}

fn check_build_failure(
    mut dozer_command: impl FnMut() -> (Command, Vec<Cleanup>),
    dozer_config_path: &str,
    message: Option<&str>,
) {
    {
        let (mut command, _cleanups) = dozer_command();
        command.args(["--config-path", dozer_config_path]);
        assert_command_fails(command, message);
    }
    {
        let (mut command, _cleanups) = dozer_command();
        command.args(["--config-path", dozer_config_path, "build"]);
        assert_command_fails(command, message);
    }
}

fn assert_command_fails(mut command: Command, expected_string_in_stdout: Option<&str>) {
    info!("Running command: {:?}", command);
    let output = command
        .output()
        .unwrap_or_else(|e| panic!("Failed to run command {command:?}: {e}"));
    if output.status.success() {
        panic!("Command {command:?} is expected to fail, but it succeeded");
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    error!("{}", stdout);
    if let Some(expected_string_in_stdout) = expected_string_in_stdout {
        if !stdout.contains(expected_string_in_stdout) {
            panic!(
                "Command {command:?}  stdout does not contain expected string '{expected_string_in_stdout}'"
            );
        }
    }
    info!("Command fails as expected: {:?}", command);
}
