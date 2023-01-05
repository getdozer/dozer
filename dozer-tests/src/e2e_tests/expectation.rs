use std::path::Path;

use dozer_types::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum Expectation {
    HealthyService,
}

impl Expectation {
    pub fn load_from_case_dir(case_dir: &Path) -> Option<Vec<Self>> {
        let expectations_path = find_expectations_path(case_dir)?;
        let expectations: Vec<Self> =
            dozer_types::serde_json::from_reader(std::fs::File::open(&expectations_path).expect(
                &format!("Failed to open expectations file {}", expectations_path),
            ))
            .expect(&format!(
                "Failed to deserialize expectations file {}",
                expectations_path
            ));
        Some(expectations)
    }
}

fn find_expectations_path(case_dir: &Path) -> Option<String> {
    for file_name in ["expectations.json"] {
        let expectations_path = case_dir.join(file_name);
        if expectations_path.exists() {
            return Some(
                expectations_path
                    .to_str()
                    .expect(&format!("Non-UTF8 path: {:?}", expectations_path))
                    .to_string(),
            );
        }
    }
    None
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum ErrorExpectation {
    InitFailure { message: Option<String> },
}

impl ErrorExpectation {
    pub fn load_from_case_dir(case_dir: &Path) -> Option<Self> {
        let error_expectation_path = find_error_expectation_path(case_dir)?;
        let error_expectation: Self = dozer_types::serde_json::from_reader(
            std::fs::File::open(&error_expectation_path).expect(&format!(
                "Failed to open error expectation file {}",
                error_expectation_path
            )),
        )
        .expect(&format!(
            "Failed to deserialize error expectation file {}",
            error_expectation_path
        ));
        Some(error_expectation)
    }
}

fn find_error_expectation_path(case_dir: &Path) -> Option<String> {
    for file_name in ["error.json"] {
        let error_expectation_path = case_dir.join(file_name);
        if error_expectation_path.exists() {
            return Some(
                error_expectation_path
                    .to_str()
                    .expect(&format!("Non-UTF8 path: {:?}", error_expectation_path))
                    .to_string(),
            );
        }
    }
    None
}
