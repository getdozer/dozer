use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct Cloud {
    #[prost(oneof = "UpdateCurrentVersionStrategy", tags = "1,2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_current_version_strategy: Option<UpdateCurrentVersionStrategy>,
    #[prost(optional, string, tag = "3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[prost(optional, string, tag = "4")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[prost(message, optional, tag = "5")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app: Option<AppInstance>,
    #[prost(message, optional, tag = "6")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<ApiInstance>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct AppInstance {
    #[prost(optional, string, tag = "1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct ApiInstance {
    #[prost(optional, uint32, tag = "1")]
    #[serde(
        default = "default_num_api_instances",
        skip_serializing_if = "Option::is_none"
    )]
    pub instances_count: Option<u32>,
    #[prost(optional, string, tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
    #[prost(optional, uint32, tag = "3")]
    /// The size of the volume in GB
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_size: Option<u32>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, prost::Oneof)]
pub enum UpdateCurrentVersionStrategy {
    #[prost(message, tag = "1")]
    OnCreate(()),
    #[prost(message, tag = "2")]
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
