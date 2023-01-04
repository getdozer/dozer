use std::{
    fs::File,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command},
    str::FromStr,
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
        let expectations_path = find_expectations_path(&case_dir);
        let error_expectation_path = find_error_expectation_path(&case_dir);

        if let Some(expectations_path) = expectations_path {
            self.run_test_case_with_expectations(&config_path, &expectations_path)
                .await;
        } else if let Some(error_expectation_path) = error_expectation_path {
            self.run_test_case_with_error_expectation(&config_path, &error_expectation_path)
                .await;
        } else {
            panic!("No expectations file found in case dir: {:?}", case_dir);
        }
    }

    async fn run_test_case_with_expectations(&self, config_path: &str, expectations_path: &str) {
        let config = parse_config(config_path);

        let expectations: Vec<Expectation> =
            dozer_types::serde_json::from_reader(File::open(&expectations_path).expect(&format!(
                "Failed to open expectations file {}",
                expectations_path
            )))
            .expect(&format!(
                "Failed to deserialize expectations file {}",
                expectations_path
            ));

        // Setup cleanups.
        let cleanups = vec![Cleanup::RemoveDirectory(config.home_dir)];

        // Start sources, dozer app and dozer API.
        let mut child_processes = vec![];
        child_processes.push(spawn_command(
            &self.dozer_bin,
            &["--config-path", &config_path],
        ));

        // Run test client.
        let rest = config.api.unwrap_or_default().rest.unwrap_or_default();
        let rest_endpoint = SocketAddr::from_str(&format!("{}:{}", rest.host, rest.port))
            .expect(&format!("Bad rest endpoint: {}:{}", rest.host, rest.port));
        let client = Client::new(rest_endpoint);
        for expectation in expectations {
            client.check_expectation(expectation).await;
        }

        // Stop child processes.
        for child in &mut child_processes {
            child.kill().expect("Failed to kill child process");
        }

        // To ensure `cleanups` is not dropped on async boundary.
        drop(cleanups);
    }

    async fn run_test_case_with_error_expectation(
        &self,
        config_path: &str,
        error_expectation_path: &str,
    ) {
        let error_expectation: ErrorExpectation = dozer_types::serde_json::from_reader(
            File::open(&error_expectation_path).expect(&format!(
                "Failed to open error expectations file {}",
                error_expectation_path
            )),
        )
        .expect(&format!(
            "Failed to deserialize error expectation file {}",
            error_expectation_path
        ));

        match error_expectation {
            ErrorExpectation::InitFailure { message } => {
                self.check_init_failure(&config_path, message);
            }
        }
    }

    fn check_init_failure(&self, config_path: &str, message: Option<String>) {
        let config = parse_config(config_path);
        let _cleanup = Cleanup::RemoveDirectory(config.home_dir);
        assert_command_fails(
            &self.dozer_bin,
            &["--config-path", &config_path, "init"],
            message,
        );
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
    info!("Commond done: {:?}", cmd);
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

fn assert_command_fails(bin: &str, args: &[&str], expected_string_in_stdout: Option<String>) {
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

fn find_expectations_path(case_dir: &Path) -> Option<String> {
    for file_name in ["expectations.json"] {
        let expectations_path = case_dir.join(file_name);
        if expectations_path.exists() {
            return Some(
                expectations_path
                    .to_str()
                    .expect(&format!("Non-UTF8 path: {:?}", expectations_path))
                    .to_string(),
            );
        }
    }
    None
}

fn find_error_expectation_path(case_dir: &Path) -> Option<String> {
    for file_name in ["error.json"] {
        let error_expectation_path = case_dir.join(file_name);
        if error_expectation_path.exists() {
            return Some(
                error_expectation_path
                    .to_str()
                    .expect(&format!("Non-UTF8 path: {:?}", error_expectation_path))
                    .to_string(),
            );
        }
    }
    None
}

fn parse_config(config_path: &str) -> Config {
    let config_string = std::fs::read_to_string(&config_path)
        .expect(&format!("Failed to read config file {}", config_path));
    dozer_types::serde_yaml::from_str(&config_string).expect(&format!(
        "Failed to deserialize config file {}",
        config_path
    ))
}
