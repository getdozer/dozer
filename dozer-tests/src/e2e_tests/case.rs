use std::{
    fs::read_to_string,
    path::{Path, PathBuf},
};

use dozer_types::models::app_config::Config;

use super::expectation::{ErrorExpectation, Expectation};

pub enum CaseKind {
    Expectations(Vec<Expectation>),
    ErrorExpectation(ErrorExpectation),
}

pub struct Case {
    pub case_dir: PathBuf,
    pub dozer_config_path: String,
    pub dozer_config: Config,
    pub kind: CaseKind,
}

impl Case {
    pub fn load_from_case_dir(case_dir: PathBuf) -> Self {
        let dozer_config_path = find_dozer_config_path(&case_dir);
        let dozer_config = read_to_string(&dozer_config_path).unwrap_or_else(|e| {
            panic!(
                "Cannot read dozer config file: {}: {}",
                dozer_config_path, e
            )
        });
        let dozer_config = dozer_types::serde_yaml::from_str(&dozer_config).unwrap_or_else(|e| {
            panic!(
                "Cannot parse dozer config file: {}: {}",
                dozer_config_path, e
            )
        });
        let expectations = Expectation::load_from_case_dir(&case_dir);
        let error_expectation = ErrorExpectation::load_from_case_dir(&case_dir);
        if let Some(expectations) = expectations {
            Self {
                case_dir,
                dozer_config_path,
                dozer_config,
                kind: CaseKind::Expectations(expectations),
            }
        } else if let Some(error_expectation) = error_expectation {
            Self {
                case_dir,
                dozer_config_path,
                dozer_config,
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
