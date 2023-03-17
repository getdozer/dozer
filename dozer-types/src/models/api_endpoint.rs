use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiIndex {
    #[prost(string, repeated, tag = "1")]
    pub primary_key: Vec<String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Clone)]
pub enum OnInsertResolutionTypes {
    Nothing = 0,
    Update = 1,
    Panic = 2,
}

impl fmt::Display for OnInsertResolutionTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OnInsertResolutionTypes::Nothing => write!(f, "nothing"),
            OnInsertResolutionTypes::Update => write!(f, "update"),
            OnInsertResolutionTypes::Panic => write!(f, "panic"),
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Clone)]
pub enum OnUpdateResolutionTypes {
    Nothing = 0,
    Upsert = 1,
    Panic = 2,
}

impl fmt::Display for OnUpdateResolutionTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OnUpdateResolutionTypes::Nothing => write!(f, "nothing"),
            OnUpdateResolutionTypes::Upsert => write!(f, "upsert"),
            OnUpdateResolutionTypes::Panic => write!(f, "panic"),
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Clone)]
pub enum OnDeleteResolutionTypes {
    Nothing = 0,
    Panic = 1,
}

impl fmt::Display for OnDeleteResolutionTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OnDeleteResolutionTypes::Nothing => write!(f, "nothing"),
            OnDeleteResolutionTypes::Panic => write!(f, "panic"),
        }
    }
}

#[derive(Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ConflictResolution {
    #[prost(string, optional, tag = "1")]
    pub on_insert: Option<String>,

    #[prost(string, optional, tag = "2")]
    pub on_update: Option<String>,

    #[prost(string, optional, tag = "3")]
    pub on_delete: Option<String>,
}

#[derive(Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
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
}

impl Serialize for ApiEndpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ApiEndpoint", 4)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("table_name", &self.table_name)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("index", &self.index)?;

        state.end()
    }
}
