use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct Cloud {
    #[prost(oneof = "UpdateCurrentVersionStrategy", tags = "1,2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_current_version_strategy: Option<UpdateCurrentVersionStrategy>,
    #[prost(optional, string, tag = "3")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
    #[prost(optional, string, tag = "4")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[prost(optional, string, tag = "5")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[prost(optional, uint32, tag = "6")]
    #[serde(
        default = "default_num_replicas",
        skip_serializing_if = "Option::is_none"
    )]
    pub num_replicas: Option<u32>,
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

fn default_num_replicas() -> Option<u32> {
    Some(2)
}
