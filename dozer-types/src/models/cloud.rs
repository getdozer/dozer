use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
pub struct Cloud {
    #[prost(oneof = "UpdateCurrentVersionStrategy", tags = "1,2")]
    pub update_current_version_strategy: Option<UpdateCurrentVersionStrategy>,
    #[prost(optional, string, tag = "3")]
    pub instance_type: Option<String>,
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
