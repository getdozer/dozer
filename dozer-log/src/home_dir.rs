use camino::{ReadDirUtf8, Utf8PathBuf};

#[derive(Debug, Clone)]
pub struct HomeDir {
    home_dir: Utf8PathBuf,
    cache_dir: Utf8PathBuf,
}

pub type Error = (Utf8PathBuf, std::io::Error);

impl HomeDir {
    pub fn new(home_dir: Utf8PathBuf, cache_dir: Utf8PathBuf) -> Self {
        Self {
            home_dir,
            cache_dir,
        }
    }

    pub fn create_build_dir_all(&self, build_id: BuildId) -> Result<BuildPath, Error> {
        std::fs::create_dir_all(&self.cache_dir).map_err(|e| (self.cache_dir.clone(), e))?;

        let build_path = self.get_build_path(build_id);

        std::fs::create_dir_all(&build_path.contracts_dir)
            .map_err(|e| (build_path.contracts_dir.clone(), e))?;
        std::fs::create_dir_all(&build_path.data_dir)
            .map_err(|e: std::io::Error| (build_path.data_dir.clone(), e))?;

        Ok(build_path)
    }

    pub fn find_build_path(&self, build_id: u32) -> Option<BuildPath> {
        let build_path = self.get_build_path(BuildId::from_id(build_id));
        if build_path.exists() {
            Some(build_path)
        } else {
            None
        }
    }

    pub fn find_latest_build_path(&self) -> Result<Option<BuildPath>, Error> {
        Ok(find_latest_build_id(self.home_dir.clone())?
            .map(|build_id| self.get_build_path(build_id)))
    }

    fn get_build_path(&self, build_id: BuildId) -> BuildPath {
        let build_dir = self.home_dir.join(&build_id.name);

        let contracts_dir = build_dir.join("contracts");
        let dag_path = contracts_dir.join("__dozer_pipeline.json");
        let descriptor_path = contracts_dir.join("file_descriptor_set.bin");

        let data_dir = build_dir.join("data");
        let log_dir_relative_to_data_dir = "log".into();

        BuildPath {
            id: build_id,
            contracts_dir,
            dag_path,
            descriptor_path,
            data_dir,
            log_dir_relative_to_data_dir,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BuildId {
    id: u32,
    name: String,
}

impl BuildId {
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

fn find_latest_build_id(dir: Utf8PathBuf) -> Result<Option<BuildId>, Error> {
    if !dir.exists() {
        return Ok(None);
    }

    let mut result = None;
    for sub_dir in list_sub_dir(dir)? {
        let sub_dir = sub_dir?;
        if let Some(build) = BuildId::from_name(&sub_dir.name) {
            if let Some(BuildId { id, .. }) = result {
                if build.id > id {
                    result = Some(build);
                }
            } else {
                result = Some(build);
            }
        }
    }
    Ok(result)
}

#[derive(Debug, Clone)]
pub struct BuildPath {
    pub id: BuildId,
    pub contracts_dir: Utf8PathBuf,
    pub dag_path: Utf8PathBuf,
    pub descriptor_path: Utf8PathBuf,
    pub data_dir: Utf8PathBuf,
    log_dir_relative_to_data_dir: Utf8PathBuf,
}

impl BuildPath {
    pub fn get_endpoint_path(&self, endpoint_name: &str) -> EndpointPath {
        let log_dir_relative_to_data_dir = self.log_dir_relative_to_data_dir.join(endpoint_name);
        EndpointPath {
            build_id: self.id.clone(),
            log_dir_relative_to_data_dir,
        }
    }

    fn exists(&self) -> bool {
        self.contracts_dir.exists() && self.data_dir.exists()
    }
}

#[derive(Debug, Clone)]
pub struct EndpointPath {
    pub build_id: BuildId,
    pub log_dir_relative_to_data_dir: Utf8PathBuf,
}
