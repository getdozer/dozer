use std::{path::Path, process::Command};

use dozer_types::log::{error, info};

use crate::Cleanup;

pub fn run_command(bin: &str, args: &[&str], on_error_command: Option<(&str, &[&str])>) {
    let mut cmd = Command::new(bin);
    cmd.args(args);
    info!("Running command: {:?}", cmd);
    let output = cmd
        .output()
        .unwrap_or_else(|e| panic!("Failed to run command {cmd:?}: {e}"));
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!("{stderr}");
        if let Some((bin, args)) = on_error_command {
            let mut cmd = Command::new(bin);
            cmd.args(args);
            let output = cmd
                .output()
                .unwrap_or_else(|e| panic!("Failed to run command {cmd:?}: {e}"));
            let stdout = String::from_utf8_lossy(&output.stdout);
            error!("{stdout}");
        }
        panic!(
            "Command {:?} failed with status {}, working directory {:?}",
            cmd,
            output.status,
            std::env::current_dir().expect("Failed to get cwd")
        );
    }
    info!("Command done: {:?}", cmd);
}

pub fn run_docker_compose(docker_compose_path: &Path, service_name: &str) -> Cleanup {
    let docker_compose_path = docker_compose_path
        .to_str()
        .unwrap_or_else(|| panic!("Non-UFT8 path {docker_compose_path:?}"));
    run_command(
        "docker",
        &[
            "compose",
            "-f",
            docker_compose_path,
            "run",
            "--build",
            service_name,
        ],
        Some((
            "docker",
            &["compose", "-f", docker_compose_path, "logs", "--no-color"],
        )),
    );
    Cleanup::DockerCompose(docker_compose_path.to_string())
}
