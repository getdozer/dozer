use crate::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message)]
pub struct UdfConfig {
    #[prost(string, tag = "1")]
    /// name of the model function
    pub name: String,
    #[prost(oneof = "UdfType", tags = "2")]
    #[serde(default = "default_udf_type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    /// setting for what type of udf to use; Default: Onnx
    pub config: Option<UdfType>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof)]
pub enum UdfType {
    #[prost(message, tag = "2")]
    Onnx(OnnxConfig),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct OnnxConfig {
    #[prost(string)]
    /// path to the model file
    pub path: String,
}

fn default_udf_type() -> Option<UdfType> {
    Some(UdfType::Onnx(OnnxConfig::default()))
}
