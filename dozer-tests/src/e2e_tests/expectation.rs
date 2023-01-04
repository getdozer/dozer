use dozer_types::serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum Expectation {
    HealthyService,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum ErrorExpectation {
    InitFailure { message: Option<String> },
}
