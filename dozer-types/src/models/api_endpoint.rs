use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

use serde_yaml::Value;

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ApiIndex {
    #[prost(string, repeated, tag = "1")]
    pub primary_key: Vec<String>,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, ::prost::Enumeration)]
#[repr(i32)]
pub enum OnInsertResolutionTypes {
    Nothing = 0,
    Update = 1,
    Panic = 2,
}

impl From<i32> for OnInsertResolutionTypes {
    fn from(v: i32) -> Self {
        match v {
            typ if typ == Self::Nothing as i32 => Self::Nothing,
            typ if typ == Self::Update as i32 => Self::Update,
            typ if typ == Self::Panic as i32 => Self::Panic,
            _ => Self::default(),
        }
    }
}

impl From<String> for OnInsertResolutionTypes {
    fn from(v: String) -> Self {
        match v.as_str() {
            "nothing" => Self::Nothing,
            "update" => Self::Update,
            "panic" => Self::Panic,
            _ => Self::default(),
        }
    }
}

impl<'de> Deserialize<'de> for OnInsertResolutionTypes {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let variant = String::deserialize(de)?;
        Ok(match variant.as_str() {
            "nothing" => Self::Nothing,
            "update" => Self::Update,
            "panic" => Self::Panic,
            _other => Self::default(),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, ::prost::Enumeration)]
pub enum OnUpdateResolutionTypes {
    Nothing = 0,
    Upsert = 1,
    Panic = 2,
}

impl fmt::Display for OnUpdateResolutionTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Nothing => write!(f, "nothing"),
            Self::Upsert => write!(f, "upsert"),
            Self::Panic => write!(f, "panic"),
        }
    }
}

impl From<String> for OnUpdateResolutionTypes {
    fn from(v: String) -> Self {
        match v.as_str() {
            "nothing" => Self::Nothing,
            "upsert" => Self::Upsert,
            "panic" => Self::Panic,
            _ => Self::default(),
        }
    }
}

impl From<i32> for OnUpdateResolutionTypes {
    fn from(v: i32) -> Self {
        match v {
            typ if typ == Self::Nothing as i32 => Self::Nothing,
            typ if typ == Self::Upsert as i32 => Self::Upsert,
            typ if typ == Self::Panic as i32 => Self::Panic,
            _ => Self::default(),
        }
    }
}

impl<'de> Deserialize<'de> for OnUpdateResolutionTypes {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let variant = String::deserialize(de)?;
        Ok(match variant.as_str() {
            "nothing" => Self::Nothing,
            "upsert" => Self::Upsert,
            "panic" => Self::Panic,
            _other => Self::default(),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, ::prost::Enumeration)]
#[repr(i32)]
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

impl From<i32> for OnDeleteResolutionTypes {
    fn from(v: i32) -> Self {
        match v {
            typ if typ == Self::Nothing as i32 => Self::Nothing,
            typ if typ == Self::Panic as i32 => Self::Panic,
            _ => Self::default(),
        }
    }
}

impl From<String> for OnDeleteResolutionTypes {
    fn from(v: String) -> Self {
        match v.as_str() {
            "nothing" => Self::Nothing,
            "panic" => Self::Panic,
            _ => Self::default(),
        }
    }
}

impl<'de> Deserialize<'de> for OnDeleteResolutionTypes {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let variant = String::deserialize(de)?;
        Ok(match variant.as_str() {
            "nothing" => Self::Nothing,
            "panic" => Self::Panic,
            _other => Self::default(),
        })
    }
}

#[derive(Copy, Eq, PartialEq, Clone, ::prost::Message)]
pub struct ConflictResolution {
    #[prost(enumeration = "OnInsertResolutionTypes", tag = "1")]
    pub on_insert: i32,

    #[prost(enumeration = "OnUpdateResolutionTypes", tag = "2")]
    pub on_update: i32,

    #[prost(enumeration = "OnDeleteResolutionTypes", tag = "3")]
    pub on_delete: i32,
}

impl<'de> Deserialize<'de> for ConflictResolution {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let variant = Value::deserialize(de)?;
        let res = ConflictResolution {
            on_insert: variant.get("on_insert").map_or_else(
                || OnInsertResolutionTypes::Nothing as i32,
                |r| {
                    r.as_str()
                        .map_or_else(OnInsertResolutionTypes::default, |s| {
                            OnInsertResolutionTypes::from(s.to_string())
                        }) as i32
                },
            ),
            on_update: variant.get("on_update").map_or_else(
                || OnUpdateResolutionTypes::Nothing as i32,
                |r| {
                    r.as_str()
                        .map_or_else(OnUpdateResolutionTypes::default, |s| {
                            OnUpdateResolutionTypes::from(s.to_string())
                        }) as i32
                },
            ),
            on_delete: variant.get("on_delete").map_or_else(
                || OnDeleteResolutionTypes::Nothing as i32,
                |r| {
                    r.as_str()
                        .map_or_else(OnDeleteResolutionTypes::default, |s| {
                            OnDeleteResolutionTypes::from(s.to_string())
                        }) as i32
                },
            ),
        };
        Ok(res)
    }
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
