use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Cloud {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_current_version_strategy: Option<UpdateCurrentVersionStrategy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<AppInstance>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<ApiInstance>,
}

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AppInstance {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
}

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ApiInstance {
    #[serde(
        default = "default_num_api_instances",
        skip_serializing_if = "Option::is_none"
    )]
    pub instances_count: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,

    /// The size of the volume in GB
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_size: Option<u32>,
}

#[derive(Debug, JsonSchema, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdateCurrentVersionStrategy {
    OnCreate(()),

    Manual(()),
}

impl Default for UpdateCurrentVersionStrategy {
    fn default() -> Self {
        UpdateCurrentVersionStrategy::OnCreate(())
    }
}

fn default_num_api_instances() -> Option<u32> {
    Some(2)
}
