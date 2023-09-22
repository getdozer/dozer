use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::equal_default;

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Cloud {
    #[serde(default, skip_serializing_if = "equal_default")]
    pub update_current_version_strategy: UpdateCurrentVersionStrategy,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub app: AppInstance,

    #[serde(default, skip_serializing_if = "equal_default")]
    pub api: ApiInstance,
}

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AppInstance {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
}

#[derive(Debug, JsonSchema, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ApiInstance {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instances_count: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,

    /// The size of the volume in GB
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_size: Option<u32>,
}

#[derive(Debug, JsonSchema, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub enum UpdateCurrentVersionStrategy {
    #[default]
    OnCreate,
    Manual,
}
