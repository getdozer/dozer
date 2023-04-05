use std::fmt::Display;
use std::process::Child;
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

use dozer_types::log::info;

use crate::e2e_tests::Case;

mod buildkite;
mod local;
pub mod running_env;

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
        let docker_compose = running_env::create_docker_compose_for_local_runner(case);
        self.0.run_test_case(case, docker_compose).await;
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

fn spawn_command(bin: &str, args: &[&str]) -> Child {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    info!("Spawning command: {:?}", cmd);
    let mut child = cmd
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to run command {cmd:?}: {e}"));
    // Give dozer some time to start.
    sleep(Duration::from_millis(30000));
    if let Some(exit_status) = child
        .try_wait()
        .unwrap_or_else(|e| panic!("Failed to check status of command {cmd:?}: {e}"))
    {
        if exit_status.success() {
            panic!("Service {cmd:?} is expected to run in background, but it exited immediately");
        } else {
            panic!(
                "Service {cmd:?} is expected to run in background, but it exited with status {exit_status}"
            );
        }
    }
    child
}
