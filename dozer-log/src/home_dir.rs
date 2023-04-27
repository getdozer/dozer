use std::path::{Path, PathBuf};

pub struct HomeDir {
    api_dir: PathBuf,
    cache_dir: PathBuf,
    log_dir: PathBuf,
}

impl HomeDir {
    pub fn new(home_dir: &Path, cache_dir: PathBuf) -> Self {
        let api_dir = home_dir.join("api");
        let log_dir = home_dir.join("pipeline").join("logs");
        Self {
            api_dir,
            cache_dir,
            log_dir,
        }
    }

    pub fn exists(&self) -> bool {
        self.api_dir.exists() || self.cache_dir.exists() || self.log_dir.exists()
    }

    pub fn create_dir_all<'a>(
        &self,
        endpoint_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), (PathBuf, std::io::Error)> {
        std::fs::create_dir_all(&self.cache_dir).map_err(|e| (self.cache_dir.clone(), e))?;

        for endpoint_name in endpoint_names {
            let api_dir = self.get_endpoint_api_dir(endpoint_name);
            std::fs::create_dir_all(&api_dir).map_err(|e| (api_dir, e))?;
            let log_dir = self.get_endpoint_log_dir(endpoint_name);
            std::fs::create_dir_all(&log_dir).map_err(|e| (log_dir, e))?;
        }
        Ok(())
    }

    pub fn get_endpoint_api_dir(&self, endpoint_name: &str) -> PathBuf {
        self.api_dir.join(endpoint_name)
    }

    pub fn get_endpoint_descriptor_path(&self, endpoint_name: &str) -> PathBuf {
        self.get_endpoint_api_dir(endpoint_name)
            .join("file_descriptor_set.bin")
    }

    fn get_endpoint_log_dir(&self, endpoint_name: &str) -> PathBuf {
        self.log_dir.join(endpoint_name)
    }

    pub fn get_endpoint_schema_path(&self, endpoint_name: &str) -> PathBuf {
        self.get_endpoint_log_dir(endpoint_name).join("schema.json")
    }

    pub fn get_endpoint_log_path(&self, endpoint_name: &str) -> PathBuf {
        self.get_endpoint_log_dir(endpoint_name).join("log")
    }
}
