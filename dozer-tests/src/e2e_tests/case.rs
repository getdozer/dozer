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

pub struct Source {
    pub directory: PathBuf,
    pub service: Option<Service>,
}

pub enum CaseKind {
    Expectations(Vec<Expectation>),
    ErrorExpectation(ErrorExpectation),
}

pub struct Case {
    pub case_dir: PathBuf,
    pub dozer_config_path: String,
    pub dozer_config: Config,
    pub sources_dir: PathBuf,
    pub sources: HashMap<String, Source>,
    pub kind: CaseKind,
}

impl Case {
    pub fn load_from_case_dir(case_dir: PathBuf, sources_dir: PathBuf) -> Self {
        let dozer_config_path = find_dozer_config_path(&case_dir);
        let dozer_config: Config = read_yaml(dozer_config_path.as_ref());

        let mut sources = HashMap::new();
        for connection in &dozer_config.connections {
            let source_dir = sources_dir.join(&connection.name);
            if !source_dir.exists() {
                continue;
            }

            if !source_dir.join("Dockerfile").exists() {
                panic!("Source {:?} must have Dockerfile", source_dir);
            }

            let service_path = find_service_path(&source_dir);
            let service = service_path
                .as_ref()
                .map(|service_path| read_yaml(service_path));

            sources.insert(
                connection.name.clone(),
                Source {
                    directory: source_dir,
                    service,
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
                sources_dir,
                sources,
                kind: CaseKind::Expectations(expectations),
            }
        } else if let Some(error_expectation) = error_expectation {
            Self {
                case_dir,
                dozer_config_path,
                dozer_config,
                sources_dir,
                sources,
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

fn find_service_path(source_dir: &Path) -> Option<PathBuf> {
    let file_name = "service.yaml";
    let service_path = source_dir.join(file_name);
    if service_path.exists() {
        Some(service_path)
    } else {
        None
    }
}
