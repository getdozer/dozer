use dozer_cli::cli::load_config_from_file;
use dozer_types::constants::DEFAULT_CONFIG_PATH;
use std::{
    collections::HashMap,
    fs::read_to_string,
    path::{Path, PathBuf},
};

use dozer_types::models::config::Config;

use super::{
    docker_compose::Service,
    expectation::{ErrorExpectation, Expectation},
};

pub struct Connection {
    pub directory: PathBuf,
    pub service: Option<Service>,
    pub has_docker_file: bool,
    pub has_oneshot_file: bool,
}

pub enum CaseKind {
    Expectations(Vec<Expectation>),
    ErrorExpectation(ErrorExpectation),
}

pub struct Case {
    pub case_dir: PathBuf,
    pub dozer_config_path: String,
    pub dozer_config: Config,
    pub connections_dir: PathBuf,
    pub connections: HashMap<String, Connection>,
    pub kind: CaseKind,
}

impl Case {
    pub fn load_from_case_dir(case_dir: PathBuf, connections_dir: PathBuf) -> Self {
        let dozer_config_path = find_dozer_config_path(&case_dir);
        let (dozer_config, _) = load_config_from_file(vec![dozer_config_path.clone()], false)
            .unwrap_or_else(|e| panic!("Cannot read file: {}: {:?}", &dozer_config_path, e));
        let mut connections = HashMap::new();
        for connection in &dozer_config.connections {
            let connection_dir = connections_dir.join(&connection.name);
            if !connection_dir.exists() {
                continue;
            }

            let docker_file = connection_dir.join("Dockerfile");

            let service_path = find_service_path(&connection_dir);
            let service = service_path
                .as_ref()
                .map(|service_path| read_yaml(service_path));

            if !docker_file.exists() && service.is_none() {
                panic!("Connection {connection_dir:?} must have either service.yaml or Dockerfile");
            }

            let oneshot_file = connection_dir.join("oneshot");

            connections.insert(
                connection.name.clone(),
                Connection {
                    directory: connection_dir,
                    service,
                    has_docker_file: docker_file.exists(),
                    has_oneshot_file: oneshot_file.exists(),
                },
            );
        }

        let expectations = Expectation::load_from_case_dir(&case_dir);
        let error_expectation = ErrorExpectation::load_from_case_dir(&case_dir);
        if let Some(expectations) = expectations {
            Self {
                case_dir,
                dozer_config_path,
                dozer_config,
                connections_dir,
                connections,
                kind: CaseKind::Expectations(expectations),
            }
        } else if let Some(error_expectation) = error_expectation {
            Self {
                case_dir,
                dozer_config_path,
                dozer_config,
                connections_dir,
                connections,
                kind: CaseKind::ErrorExpectation(error_expectation),
            }
        } else {
            panic!("Case {case_dir:?} must have either expectations or error expectation");
        }
    }

    pub fn should_be_ignored(&self) -> bool {
        // Check if the last component of `case_dir` starts with `ignore-`.
        self.case_dir
            .file_name()
            .unwrap_or_else(|| {
                panic!(
                    "Case directory must have a file name, but it's {:?}",
                    self.case_dir
                )
            })
            .to_str()
            .unwrap_or_else(|| panic!("Non-UTF8 path: {:?}", self.case_dir))
            .starts_with("ignore-")
    }
}

fn find_dozer_config_path(case_dir: &Path) -> String {
    {
        let config_path = case_dir.join(DEFAULT_CONFIG_PATH);
        if config_path.exists() {
            return config_path
                .to_str()
                .unwrap_or_else(|| panic!("Non-UTF8 path: {config_path:?}"))
                .to_string();
        }
    }
    panic!("Cannot find config file in case directory: {case_dir:?}");
}

fn read_yaml<T: dozer_types::serde::de::DeserializeOwned>(path: &Path) -> T {
    let string = read_to_string(path)
        .unwrap_or_else(|e| panic!("Cannot read file: {}: {}", path.to_string_lossy(), e));
    dozer_types::serde_yaml::from_str(&string)
        .unwrap_or_else(|e| panic!("Cannot parse file: {}: {}", path.to_string_lossy(), e))
}

fn find_service_path(connection_dir: &Path) -> Option<PathBuf> {
    let file_name = "service.yaml";
    let service_path = connection_dir.join(file_name);
    if service_path.exists() {
        Some(service_path)
    } else {
        None
    }
}
