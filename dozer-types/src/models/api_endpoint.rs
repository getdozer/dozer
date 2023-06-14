use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiIndex {
    #[prost(string, repeated, tag = "1")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,
    #[prost(message, tag = "2")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary: Option<SecondaryIndexConfig>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct SecondaryIndexConfig {
    #[prost(string, repeated, tag = "1")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_default: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub create: Vec<CreateSecondaryIndex>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct CreateSecondaryIndex {
    #[prost(oneof = "SecondaryIndex", tags = "1,2")]
    pub index: Option<SecondaryIndex>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof)]
pub enum SecondaryIndex {
    #[prost(message, tag = "1")]
    SortedInverted(SortedInverted),
    #[prost(message, tag = "2")]
    FullText(FullText),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct SortedInverted {
    #[prost(string, repeated, tag = "1")]
    pub fields: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct FullText {
    #[prost(string, tag = "1")]
    pub field: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Copy, ::prost::Oneof)]
pub enum OnInsertResolutionTypes {
    #[prost(message, tag = "1")]
    Nothing(()),
    #[prost(message, tag = "2")]
    Update(()),
    #[prost(message, tag = "3")]
    Panic(()),
}

impl Default for OnInsertResolutionTypes {
    fn default() -> Self {
        OnInsertResolutionTypes::Nothing(())
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Copy, ::prost::Oneof)]
pub enum OnUpdateResolutionTypes {
    #[prost(message, tag = "1")]
    Nothing(()),
    #[prost(message, tag = "2")]
    Upsert(()),
    #[prost(message, tag = "3")]
    Panic(()),
}

impl Default for OnUpdateResolutionTypes {
    fn default() -> Self {
        OnUpdateResolutionTypes::Nothing(())
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Copy, ::prost::Oneof)]
pub enum OnDeleteResolutionTypes {
    #[prost(message, tag = "1")]
    Nothing(()),
    #[prost(message, tag = "2")]
    Panic(()),
}

impl Default for OnDeleteResolutionTypes {
    fn default() -> Self {
        OnDeleteResolutionTypes::Nothing(())
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Copy, ::prost::Message)]
pub struct ConflictResolution {
    #[prost(oneof = "OnInsertResolutionTypes", tags = "1,2,3")]
    pub on_insert: Option<OnInsertResolutionTypes>,

    #[prost(oneof = "OnUpdateResolutionTypes", tags = "4,5,6")]
    pub on_update: Option<OnUpdateResolutionTypes>,

    #[prost(oneof = "OnDeleteResolutionTypes", tags = "7,8")]
    pub on_delete: Option<OnDeleteResolutionTypes>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiEndpoint {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    /// name of the table in source database; Type: String
    pub table_name: String,

    #[prost(string, tag = "3")]
    /// path of endpoint - e.g: /stocks
    pub path: String,
    #[prost(message, tag = "4")]
    pub index: Option<ApiIndex>,

    #[prost(message, tag = "5")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_resolution: Option<ConflictResolution>,

    #[prost(optional, uint32, tag = "6")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}
