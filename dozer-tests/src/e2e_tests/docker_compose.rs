use std::collections::HashMap;

use dozer_types::serde::{Deserialize, Serialize};

#[derive(Serialize)]
#[serde(crate = "dozer_types::serde")]
pub struct DockerCompose {
    version: String,
    services: HashMap<String, Service>,
}

impl DockerCompose {
    pub fn new_v2_4(services: HashMap<String, Service>) -> Self {
        Self {
            version: "2.4".to_string(),
            services,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(crate = "dozer_types::serde")]
pub struct Service {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build: Option<Build>,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Vec::new")]
    pub ports: Vec<Port>,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Vec::new")]
    pub environment: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Vec::new")]
    pub volumes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default = "HashMap::new")]
    pub depends_on: HashMap<String, DependsOn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<HealthCheck>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct Build {
    pub context: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dockerfile: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct Port {
    pub target: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<Mode>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Host,
    Ingress,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct DependsOn {
    pub condition: Condition,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum Condition {
    ServiceStarted,
    ServiceHealthy,
    ServiceCompletedSuccessfully,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct HealthCheck {
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Vec::new")]
    pub test: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
}
