use camino::Utf8PathBuf;

#[derive(Debug, Clone)]
pub struct HomeDir {
    home_dir: Utf8PathBuf,
}

pub type Error = (Utf8PathBuf, std::io::Error);

impl HomeDir {
    pub fn new(home_dir: Utf8PathBuf) -> Self {
        Self { home_dir }
    }

    pub fn create_build_dir_all(&self, build_id: BuildId) -> Result<BuildPath, Error> {
        let build_path = self.get_build_path(build_id);

        std::fs::create_dir_all(&build_path.contracts_dir)
            .map_err(|e| (build_path.contracts_dir.clone(), e))?;
        std::fs::create_dir_all(&build_path.data_dir)
            .map_err(|e: std::io::Error| (build_path.data_dir.clone(), e))?;

        Ok(build_path)
    }

    fn get_build_path(&self, build_id: BuildId) -> BuildPath {
        let build_dir = self.home_dir.join(&build_id.name);

        let contracts_dir = build_dir.join("contracts");
        let descriptor_path = contracts_dir.join("file_descriptor_set.bin");

        let data_dir = build_dir.join("data");

        BuildPath {
            id: build_id,
            contracts_dir,
            descriptor_path,
            data_dir,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BuildId {
    name: String,
}

impl BuildId {
    fn from_id(id: u32) -> Self {
        Self {
            name: format!("v{id:04}"),
        }
    }

    pub fn first() -> Self {
        Self::from_id(1)
    }
}

#[derive(Debug, Clone)]
pub struct BuildPath {
    pub id: BuildId,
    pub contracts_dir: Utf8PathBuf,
    pub descriptor_path: Utf8PathBuf,
    pub data_dir: Utf8PathBuf,
}
