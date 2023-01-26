use std::{
    collections::HashMap,
    fs::read_to_string,
    path::{Path, PathBuf},
};

use dozer_types::models::app_config::Config;

use super::{
    docker_compose::Service,
    expectation::{ErrorExpectation, Expectation},
};

pub struct Connection {
    pub directory: PathBuf,
    pub service: Option<Service>,
    pub has_docker_file: bool,
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
        let dozer_config: Config = read_yaml(dozer_config_path.as_ref());

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
                panic!(
                    "Connection {:?} must have either service.yaml or Dockerfile",
                    connection_dir
                );
            }

            connections.insert(
                connection.name.clone(),
                Connection {
                    directory: connection_dir,
                    service,
                    has_docker_file: docker_file.exists(),
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
            panic!(
                "Case {:?} must have either expectations or error expectation",
                case_dir
            );
        }
    }
}

fn find_dozer_config_path(case_dir: &Path) -> String {
    {
        let file_name = "dozer-config.yaml";
        let config_path = case_dir.join(file_name);
        if config_path.exists() {
            return config_path
                .to_str()
                .unwrap_or_else(|| panic!("Non-UTF8 path: {:?}", config_path))
                .to_string();
        }
    }
    panic!("Cannot find config file in case directory: {:?}", case_dir);
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
