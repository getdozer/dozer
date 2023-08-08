use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message, Hash)]
pub struct UdfConfig {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(oneof = "UdfTypeConfig", tag = "2")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<UdfTypeConfig>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Oneof, Hash)]
pub enum UdfTypeConfig {
    #[prost(message, tag = "1")]
    /// In yaml, present as tag: `!Onnx`
    Onnx(OnnxConfig),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, prost::Message, Hash)]
pub struct OnnxConfig {
    #[prost(string, tag = "1")]
    pub path: String,
}
