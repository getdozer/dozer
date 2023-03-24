use std::process::{Child, Command};

use dozer_types::log::error;

#[must_use]
pub enum Cleanup {
    RemoveDirectory(String),
    KillProcess(Child),
    DockerCompose(String),
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        match self {
            Cleanup::RemoveDirectory(dir) => {
                if let Err(e) = std::fs::remove_dir_all(&dir) {
                    error!("Failed to remove directory {}: {}", dir, e);
                }
            }
            Cleanup::KillProcess(child) => {
                if let Err(e) = child.kill() {
                    error!("Failed to kill process {}: {}", child.id(), e);
                }
            }
            Cleanup::DockerCompose(docker_compose_path) => {
                match Command::new("docker")
                    .args(["compose", "-f", docker_compose_path, "down"])
                    .status()
                {
                    Ok(status) => {
                        if !status.success() {
                            error!(
                                "docker compose down with input file {} failed with status {}",
                                docker_compose_path, status
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to run docker compose down with input file {}: {}",
                            docker_compose_path, e
                        );
                    }
                }
            }
        }
    }
}
