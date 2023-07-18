use std::{path::Path, process::Command};

use dozer_utils::{
    process::{run_command, run_docker_compose},
    Cleanup,
};

use crate::e2e_tests::{Case, CaseKind};

use super::{
    super::{checker::check_error_expectation, run_test_client},
    running_env::LocalDockerCompose,
    spawn_command,
};

pub struct Runner {
    dozer_bin: String,
}

impl Runner {
    pub fn new() -> Self {
        match std::env::var("DOZER_BIN") {
            Ok(dozer_bin) => Self { dozer_bin },
            Err(_) => {
                let dozer_bin = "./target/debug/dozer".to_string();
                if !AsRef::<Path>::as_ref(&dozer_bin).exists() {
                    panic!(
                        "dozer binary not found at {dozer_bin}. Did you run `cargo build --bin dozer`?"
                    );
                }
                Self { dozer_bin }
            }
        }
    }

    pub async fn run_test_case(&self, case: &Case, docker_compose: Option<LocalDockerCompose>) {
        match &case.kind {
            CaseKind::Expectations(expectations) => {
                for spawn_dozer in [spawn_dozer_same_process, spawn_dozer_two_processes] {
                    let mut cleanups = vec![];

                    // Start docker compose if necessary.
                    if let Some(docker_compose) = &docker_compose {
                        cleanups.push(run_docker_compose(
                            &docker_compose.path,
                            &docker_compose.connections_healthy_service_name,
                        ));
                    }

                    // Start dozer.
                    cleanups.push(Cleanup::RemoveDirectory(case.dozer_config.home_dir.clone()));
                    cleanups.extend(spawn_dozer(&self.dozer_bin, &case.dozer_config_path));

                    // Run test case.
                    run_test_client(case.dozer_config.clone(), expectations).await;

                    // To ensure `cleanups` is not dropped at await point.
                    drop(cleanups);
                }
            }
            CaseKind::ErrorExpectation(error_expectation) => {
                check_error_expectation(
                    || {
                        let mut cleanups = vec![];
                        if let Some(docker_compose) = &docker_compose {
                            cleanups.push(run_docker_compose(
                                &docker_compose.path,
                                &docker_compose.connections_healthy_service_name,
                            ));
                        }
                        cleanups.push(Cleanup::RemoveDirectory(case.dozer_config.home_dir.clone()));

                        (Command::new(&self.dozer_bin), cleanups)
                    },
                    &case.dozer_config_path,
                    error_expectation,
                )
                .await;
            }
        }
    }
}

fn spawn_dozer_same_process(dozer_bin: &str, dozer_config_path: &str) -> Vec<Cleanup> {
    let child = spawn_command(dozer_bin, &["--config-path", dozer_config_path]);
    vec![Cleanup::KillProcess(child)]
}

fn spawn_dozer_two_processes(dozer_bin: &str, dozer_config_path: &str) -> Vec<Cleanup> {
    run_command(
        dozer_bin,
        &["--config-path", dozer_config_path, "build"],
        None,
    );
    let mut cleanups = vec![];
    let child = spawn_command(
        dozer_bin,
        &["--config-path", dozer_config_path, "run", "app"],
    );
    cleanups.push(Cleanup::KillProcess(child));
    let child = spawn_command(
        dozer_bin,
        &["--config-path", dozer_config_path, "run", "api"],
    );
    cleanups.push(Cleanup::KillProcess(child));
    cleanups
}
