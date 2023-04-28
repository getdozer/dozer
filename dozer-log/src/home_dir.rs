use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct HomeDir {
    api_dir: PathBuf,
    cache_dir: PathBuf,
    log_dir: PathBuf,
}

pub type Error = (PathBuf, std::io::Error);

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

    pub fn create_new_migration(&self, endpoint_name: &str) -> Result<MigrationPath, Error> {
        std::fs::create_dir_all(&self.cache_dir).map_err(|e| (self.cache_dir.clone(), e))?;

        let id = self
            .find_latest_migration_id(endpoint_name)?
            .map(|migration_id| migration_id.id)
            .unwrap_or(0);
        let migration_id = MigrationId::from_id(id + 1);
        let migration_path = self.get_migration_path(endpoint_name, &migration_id);

        std::fs::create_dir_all(&migration_path.api_dir)
            .map_err(|e| (migration_path.api_dir.clone(), e))?;
        std::fs::create_dir_all(&migration_path.log_dir)
            .map_err(|e: std::io::Error| (migration_path.log_dir.clone(), e))?;

        Ok(migration_path)
    }

    pub fn find_latest_migration_path(
        &self,
        endpoint_name: &str,
    ) -> Result<Option<MigrationPath>, Error> {
        Ok(self
            .find_latest_migration_id(endpoint_name)?
            .map(|migration_id| self.get_migration_path(endpoint_name, &migration_id)))
    }

    fn find_latest_migration_id(&self, endpoint_name: &str) -> Result<Option<MigrationId>, Error> {
        let api_dir = self.get_endpoint_api_dir(endpoint_name);
        let migration1 = find_latest_migration_id(&api_dir)?;
        let log_dir = self.get_endpoint_log_dir(endpoint_name);
        let migration2 = find_latest_migration_id(&log_dir)?;

        match (migration1, migration2) {
            (Some(migration1), Some(migration2)) => {
                if migration1.id > migration2.id {
                    Ok(Some(migration1))
                } else {
                    Ok(Some(migration2))
                }
            }
            (Some(migration1), None) => Ok(Some(migration1)),
            (None, Some(migration2)) => Ok(Some(migration2)),
            (None, None) => Ok(None),
        }
    }

    fn get_migration_path(&self, endpoint_name: &str, migration_id: &MigrationId) -> MigrationPath {
        let api_dir = self
            .get_endpoint_api_dir(endpoint_name)
            .join(&migration_id.name);
        let descriptor_path = api_dir.join("file_descriptor_set.bin");
        let log_dir = self
            .get_endpoint_log_dir(endpoint_name)
            .join(&migration_id.name);
        let schema_path = log_dir.join("schema.json");
        let log_path = log_dir.join("log");
        MigrationPath {
            api_dir,
            descriptor_path,
            log_dir,
            schema_path,
            log_path,
        }
    }

    fn get_endpoint_api_dir(&self, endpoint_name: &str) -> PathBuf {
        self.api_dir.join(endpoint_name)
    }

    fn get_endpoint_log_dir(&self, endpoint_name: &str) -> PathBuf {
        self.log_dir.join(endpoint_name)
    }
}

#[derive(Debug, Clone)]
pub struct MigrationId {
    id: u32,
    name: OsString,
}

impl MigrationId {
    fn from_id(id: u32) -> Self {
        Self {
            id,
            name: OsString::from(format!("v{:04}", id)),
        }
    }

    fn from_name(name: OsString) -> Option<Self> {
        let id = name
            .to_str()
            .and_then(|s| s.strip_prefix('v'))
            .and_then(|s| s.parse::<u32>().ok())?;
        Some(Self { id, name })
    }
}

fn find_latest_migration_id(dir: &Path) -> Result<Option<MigrationId>, Error> {
    if !dir.exists() {
        return Ok(None);
    }

    let mut result = None;
    for entry in std::fs::read_dir(dir).map_err(|e| (dir.to_path_buf(), e))? {
        let entry = entry.map_err(|e| (dir.to_path_buf(), e))?;
        if entry.path().is_dir() {
            if let Some(migration) = MigrationId::from_name(entry.file_name()) {
                if let Some(MigrationId { id, .. }) = result {
                    if migration.id > id {
                        result = Some(migration);
                    }
                } else {
                    result = Some(migration);
                }
            }
        }
    }
    Ok(result)
}

#[derive(Debug, Clone)]
pub struct MigrationPath {
    pub api_dir: PathBuf,
    pub descriptor_path: PathBuf,
    log_dir: PathBuf,
    pub schema_path: PathBuf,
    pub log_path: PathBuf,
}
