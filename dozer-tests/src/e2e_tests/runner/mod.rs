use std::fmt::Display;
use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

use dozer_types::log::{error, info};

use crate::e2e_tests::Case;

use super::cleanup::Cleanup;

mod buildkite;
mod local;
mod running_env;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum)]
pub enum RunnerType {
    Local,
    Buildkite,
}

impl Display for RunnerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunnerType::Local => write!(f, "local"),
            RunnerType::Buildkite => write!(f, "buildkite"),
        }
    }
}

#[dozer_api::async_trait::async_trait]
pub trait Runner {
    async fn run_test_case(&self, case: &Case);
}

pub fn create_runner(runner_type: RunnerType) -> Box<dyn Runner> {
    match runner_type {
        RunnerType::Local => Box::new(LocalRunner(local::Runner::new())),
        RunnerType::Buildkite => Box::new(BuildkiteRunner(buildkite::Runner)),
    }
}

struct LocalRunner(local::Runner);

#[dozer_api::async_trait::async_trait]
impl Runner for LocalRunner {
    async fn run_test_case(&self, case: &Case) {
        let docker_compose_path = running_env::create_docker_compose_for_local_runner(case);
        self.0.run_test_case(case, docker_compose_path).await;
    }
}

struct BuildkiteRunner(buildkite::Runner);

#[dozer_api::async_trait::async_trait]
impl Runner for BuildkiteRunner {
    async fn run_test_case(&self, case: &Case) {
        for running_env in running_env::create_buildkite_runner_envs(case) {
            self.0.run_test_case(case, running_env).await;
        }
    }
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

fn spawn_docker_compose(docker_compose_path: &Path) -> Cleanup {
    let docker_compose_path = docker_compose_path
        .to_str()
        .unwrap_or_else(|| panic!("Non-UFT8 path {:?}", docker_compose_path));
    spawn_command("docker", &["compose", "-f", docker_compose_path, "up"]);
    Cleanup::DockerCompose(docker_compose_path.to_string())
}

fn spawn_command(bin: &str, args: &[&str]) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    info!("Spawning command: {:?}", cmd);
    let mut child = cmd.spawn().expect("Failed to run command");
    sleep(Duration::from_millis(2000));
    if let Some(exit_status) = child
        .try_wait()
        .unwrap_or_else(|e| panic!("Failed to check status of command {:?}: {}", cmd, e))
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
