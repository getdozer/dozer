use std::process::Child;

use dozer_types::log::error;

pub enum Cleanup {
    RemoveDirectory(String),
    KillProcess(Child),
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
        }
    }
}
