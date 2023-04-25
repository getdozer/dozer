use std::process::Command;

use dozer_utils::{process::run_command, Cleanup};

use crate::e2e_tests::{checker::check_error_expectation, Case, CaseKind};

use super::running_env::RunningEnv;

pub struct Runner;

impl Runner {
    pub async fn run_test_case(&self, case: &Case, running_env: RunningEnv) {
        match running_env {
            RunningEnv::WithExpectations {
                docker_compose_path,
                dozer_test_client_service_name: dozer_tests_service_name,
            } => {
                let docker_compose_path = docker_compose_path
                    .to_str()
                    .unwrap_or_else(|| panic!("Non-UTF8 path: {docker_compose_path:?}"));
                // TODO: Upload buildkite pipeline.
                run_command(
                    "docker",
                    &["compose", "-f", docker_compose_path, "pull", "dozer"],
                    None,
                );
                let _cleanup = Cleanup::DockerCompose(docker_compose_path.to_string());
                run_command(
                    "docker",
                    &[
                        "compose",
                        "-f",
                        docker_compose_path,
                        "run",
                        "--build",
                        &dozer_tests_service_name,
                    ],
                    Some((
                        "docker",
                        &["compose", "-f", docker_compose_path, "logs", "--no-color"],
                    )),
                );
            }
            RunningEnv::WithErrorExpectation {
                docker_compose_path,
                dozer_service_name,
                dozer_config_path,
            } => {
                let CaseKind::ErrorExpectation(error_expectation) = &case.kind else {
                    panic!(
                        "Running env created for error expectation but case doesn't have one, dozer config path is {}",
                        case.dozer_config_path
                    );
                };
                let docker_compose_path = docker_compose_path
                    .to_str()
                    .unwrap_or_else(|| panic!("Non-UTF8 path: {docker_compose_path:?}"));
                run_command(
                    "docker",
                    &["compose", "-f", docker_compose_path, "pull", "dozer"],
                    None,
                );
                check_error_expectation(
                    || {
                        let mut command = Command::new("docker");
                        command.args([
                            "compose",
                            "-f",
                            docker_compose_path,
                            "run",
                            &dozer_service_name,
                            "dozer",
                        ]);
                        (
                            command,
                            vec![Cleanup::DockerCompose(docker_compose_path.to_string())],
                        )
                    },
                    &dozer_config_path,
                    error_expectation,
                )
                .await;
            }
        }
    }
}
