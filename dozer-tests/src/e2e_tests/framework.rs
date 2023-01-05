use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    thread::sleep,
    time::Duration,
};

use dozer_types::{
    log::{error, info},
    models::app_config::Config,
};

use super::client::Client;
use super::expectation::Expectation;
use super::{cleanup::Cleanup, expectation::ErrorExpectation};

pub struct Framework {
    dozer_bin: String,
}

impl Framework {
    pub fn new() -> Self {
        let dozer_bin = build_dozer();
        Self { dozer_bin }
    }

    pub async fn run_test_case(&self, case_dir: PathBuf) {
        info!("Testing case: {:?}", case_dir);

        // Find config.
        let config_path = find_config_path(&case_dir);

        // Parse expectations.
        let expectations = Expectation::load_from_case_dir(&case_dir);
        let error_expectation = ErrorExpectation::load_from_case_dir(&case_dir);

        if let Some(expectations) = expectations {
            self.run_test_case_with_expectations(&config_path, &expectations)
                .await;
        } else if let Some(error_expectation) = error_expectation {
            self.run_test_case_with_error_expectation(&config_path, &error_expectation)
                .await;
        } else {
            panic!("No expectations file found in case dir: {:?}", case_dir);
        }
    }

    async fn run_test_case_with_expectations(
        &self,
        config_path: &str,
        expectations: &[Expectation],
    ) {
        let config = parse_config(config_path);

        for spawn_dozer in [spawn_dozer_same_process, spawn_dozer_two_processes] {
            // Setup cleanups.
            let mut cleanups = vec![Cleanup::RemoveDirectory(config.home_dir.clone())];

            // Start sources, dozer app and dozer API.
            let child_processes = spawn_dozer(&self.dozer_bin, &config_path);
            for child in child_processes {
                cleanups.push(Cleanup::KillProcess(child));
            }

            // Run test client.
            let mut client = Client::new(config.clone()).await;
            for expectation in expectations {
                client.check_expectation(expectation).await;
            }

            // To ensure `cleanups` is not dropped on async boundary.
            drop(cleanups);
        }
    }

    async fn run_test_case_with_error_expectation(
        &self,
        config_path: &str,
        error_expectation: &ErrorExpectation,
    ) {
        match error_expectation {
            ErrorExpectation::InitFailure { message } => {
                self.check_init_failure(&config_path, message.as_deref());
            }
        }
    }

    fn check_init_failure(&self, config_path: &str, message: Option<&str>) {
        let config = parse_config(config_path);
        {
            let _cleanup = Cleanup::RemoveDirectory(config.home_dir.clone());
            assert_command_fails(&self.dozer_bin, &["--config-path", &config_path], message);
        }
        {
            let _cleanup = Cleanup::RemoveDirectory(config.home_dir);
            assert_command_fails(
                &self.dozer_bin,
                &["--config-path", &config_path, "init"],
                message,
            );
        }
    }
}

fn build_dozer() -> String {
    run_command(
        "cargo",
        &[
            "build",
            "--manifest-path",
            "../Cargo.toml", // Working directory is `dozer-tests`.
            "--release",
            "--bin",
            "dozer",
        ],
    );
    "../target/release/dozer".to_string()
}

fn spawn_dozer_same_process(dozer_bin: &str, config_path: &str) -> Vec<Child> {
    vec![spawn_command(dozer_bin, &["--config-path", config_path])]
}

fn spawn_dozer_two_processes(dozer_bin: &str, config_path: &str) -> Vec<Child> {
    run_command(dozer_bin, &["--config-path", config_path, "init"]);
    vec![
        spawn_command(dozer_bin, &["--config-path", config_path, "app", "run"]),
        spawn_command(dozer_bin, &["--config-path", config_path, "api", "run"]),
    ]
}

fn run_command(bin: &str, args: &[&str]) {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    info!("Running command: {:?}", cmd);
    let output = cmd.output().expect("Failed to run command");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!("{}", stderr);
        panic!(
            "Command {:?} failed with status {}, working directory {:?}",
            cmd,
            output.status,
            std::env::current_dir().expect("Failed to get cwd")
        );
    }
    info!("Command done: {:?}", cmd);
}

fn spawn_command(bin: &str, args: &[&str]) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    info!("Spawning command: {:?}", cmd);
    let mut child = cmd.spawn().expect("Failed to run command");
    sleep(Duration::from_millis(500));
    if let Some(exit_status) = child
        .try_wait()
        .expect(&format!("Failed to check status of command {:?}", cmd))
    {
        if exit_status.success() {
            panic!(
                "Service {:?} is expected to run in background, but it exited immediately",
                cmd
            );
        } else {
            panic!(
                "Service {:?} is expected to run in background, but it exited with status {}",
                cmd, exit_status
            );
        }
    }
    child
}

fn assert_command_fails(bin: &str, args: &[&str], expected_string_in_stdout: Option<&str>) {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    info!("Running command: {:?}", cmd);
    let output = cmd.output().expect("Failed to run command");
    if output.status.success() {
        panic!("Command {:?} is expected to fail, but it succeeded", cmd);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    error!("{}", stdout);
    if let Some(expected_string_in_stdout) = expected_string_in_stdout {
        if !stdout.contains(&expected_string_in_stdout) {
            panic!(
                "Command {:?}  stdout does not contain expected string '{}'",
                cmd, expected_string_in_stdout
            );
        }
    }
    info!("Command fails as expected: {:?}", cmd);
}

fn find_config_path(case_dir: &Path) -> String {
    for file_name in ["dozer-config.yaml"] {
        let config_path = case_dir.join(file_name);
        if config_path.exists() {
            return config_path
                .to_str()
                .expect(&format!("Non-UTF8 path: {:?}", config_path))
                .to_string();
        }
    }
    panic!("Cannot find config file in case directory: {:?}", case_dir);
}

fn parse_config(config_path: &str) -> Config {
    let config_string = std::fs::read_to_string(&config_path)
        .expect(&format!("Failed to read config file {}", config_path));
    dozer_types::serde_yaml::from_str(&config_string).expect(&format!(
        "Failed to deserialize config file {}",
        config_path
    ))
}
