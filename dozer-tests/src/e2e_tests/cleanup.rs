use dozer_types::log::error;

pub enum Cleanup {
    RemoveDirectory(String),
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        match self {
            Cleanup::RemoveDirectory(dir) => {
                if let Err(e) = std::fs::remove_dir_all(&dir) {
                    error!("Failed to remove directory {}: {}", dir, e);
                }
            }
        }
    }
}
