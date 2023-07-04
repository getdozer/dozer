use camino::{ReadDirUtf8, Utf8Path, Utf8PathBuf};

#[derive(Debug, Clone)]
pub struct HomeDir {
    api_dir: Utf8PathBuf,
    cache_dir: Utf8PathBuf,
    log_dir: Utf8PathBuf,
}

pub type Error = (Utf8PathBuf, std::io::Error);

impl HomeDir {
    pub fn new(home_dir: &str, cache_dir: String) -> Self {
        let home_dir = AsRef::<Utf8Path>::as_ref(home_dir);
        let api_dir = home_dir.join("api");
        let log_dir = home_dir.join("pipeline").join("logs");
        Self {
            api_dir,
            cache_dir: cache_dir.into(),
            log_dir,
        }
    }

    pub fn create_migration_dir_all(
        &self,
        endpoint_name: &str,
        migration_id: MigrationId,
    ) -> Result<MigrationPath, Error> {
        std::fs::create_dir_all(&self.cache_dir).map_err(|e| (self.cache_dir.clone(), e))?;

        let migration_path = self.get_migration_path(endpoint_name, migration_id);

        std::fs::create_dir_all(&migration_path.api_dir)
            .map_err(|e| (migration_path.api_dir.clone(), e))?;
        std::fs::create_dir_all(&migration_path.log_dir)
            .map_err(|e: std::io::Error| (migration_path.log_dir.clone(), e))?;

        Ok(migration_path)
    }

    pub fn find_migration_path(
        &self,
        endpoint_name: &str,
        migration_id: u32,
    ) -> Option<MigrationPath> {
        let migration_path =
            self.get_migration_path(endpoint_name, MigrationId::from_id(migration_id));
        if migration_path.exists() {
            Some(migration_path)
        } else {
            None
        }
    }

    pub fn find_latest_migration_path(
        &self,
        endpoint_name: &str,
    ) -> Result<Option<MigrationPath>, Error> {
        Ok(self
            .find_latest_migration_id(endpoint_name)?
            .map(|migration_id| self.get_migration_path(endpoint_name, migration_id)))
    }

    pub fn find_latest_migration_id(
        &self,
        endpoint_name: &str,
    ) -> Result<Option<MigrationId>, Error> {
        let api_dir = self.get_endpoint_api_dir(endpoint_name);
        let migration1 = find_latest_migration_id(api_dir)?;
        let log_dir = self.get_endpoint_log_dir(endpoint_name);
        let migration2 = find_latest_migration_id(log_dir)?;

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

    fn get_migration_path(&self, endpoint_name: &str, migration_id: MigrationId) -> MigrationPath {
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
            id: migration_id,
            api_dir,
            descriptor_path,
            log_dir,
            schema_path,
            log_path,
        }
    }

    fn get_endpoint_api_dir(&self, endpoint_name: &str) -> Utf8PathBuf {
        self.api_dir.join(endpoint_name)
    }

    fn get_endpoint_log_dir(&self, endpoint_name: &str) -> Utf8PathBuf {
        self.log_dir.join(endpoint_name)
    }

    pub fn list_endpoints(&self) -> Result<Vec<String>, Error> {
        if !self.api_dir.exists() || !self.log_dir.exists() {
            return Ok(vec![]);
        }

        let mut result = vec![];
        for sub_dir in list_sub_dir(self.api_dir.clone())? {
            let sub_dir = sub_dir?;
            let log_dir = self.get_endpoint_log_dir(&sub_dir.name);
            if !log_dir.is_dir() {
                continue;
            }
            result.push(sub_dir.name);
        }
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct MigrationId {
    id: u32,
    name: String,
}

impl MigrationId {
    fn from_id(id: u32) -> Self {
        Self {
            id,
            name: format!("v{id:04}"),
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        let id = name.strip_prefix('v').and_then(|s| s.parse::<u32>().ok())?;
        Some(Self {
            id,
            name: name.to_string(),
        })
    }

    pub fn first() -> Self {
        Self::from_id(1)
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn next(&self) -> Self {
        Self::from_id(self.id + 1)
    }
}

struct ListSubDir {
    parent_dir: Utf8PathBuf,
    read_dir: ReadDirUtf8,
}

struct SubDir {
    _path: Utf8PathBuf,
    name: String,
}

impl Iterator for ListSubDir {
    type Item = Result<SubDir, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = self.read_dir.next()?;
            let entry = match entry {
                Err(e) => return Some(Err((self.parent_dir.clone(), e))),
                Ok(entry) => entry,
            };

            let path = entry.path();
            if path.is_dir() {
                return Some(Ok(SubDir {
                    _path: path.to_path_buf(),
                    name: entry.file_name().to_string(),
                }));
            }
        }
    }
}

fn list_sub_dir(dir: Utf8PathBuf) -> Result<ListSubDir, Error> {
    let read_dir = dir.read_dir_utf8().map_err(|e| (dir.clone(), e))?;
    Ok(ListSubDir {
        parent_dir: dir,
        read_dir,
    })
}

fn find_latest_migration_id(dir: Utf8PathBuf) -> Result<Option<MigrationId>, Error> {
    if !dir.exists() {
        return Ok(None);
    }

    let mut result = None;
    for sub_dir in list_sub_dir(dir)? {
        let sub_dir = sub_dir?;
        if let Some(migration) = MigrationId::from_name(&sub_dir.name) {
            if let Some(MigrationId { id, .. }) = result {
                if migration.id > id {
                    result = Some(migration);
                }
            } else {
                result = Some(migration);
            }
        }
    }
    Ok(result)
}

#[derive(Debug, Clone)]
pub struct MigrationPath {
    pub id: MigrationId,
    pub api_dir: Utf8PathBuf,
    pub descriptor_path: Utf8PathBuf,
    log_dir: Utf8PathBuf,
    pub schema_path: Utf8PathBuf,
    pub log_path: Utf8PathBuf,
}

impl MigrationPath {
    pub fn exists(&self) -> bool {
        self.api_dir.exists() && self.log_dir.exists()
    }
}
