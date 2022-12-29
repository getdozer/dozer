use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    errors::internal::BoxedError,
    types::{Commit, OperationEvent, Schema},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestionOperation {
    OperationEvent(OperationEvent),
    // Table Name, Schema
    SchemaUpdate(String, Schema),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum IngestionMessage {
    Begin(),
    OperationEvent(OperationEvent),
    // Table Name, Schema
    Schema(String, Schema),
    Commit(Commit),
}

#[derive(Error, Debug)]
pub enum IngestorError {
    #[error("Failed to send message on channel")]
    ChannelError(#[from] BoxedError),
}

pub trait IngestorForwarder: Send + Sync + Debug {
    fn forward(&self, msg: (u64, IngestionOperation)) -> Result<(), IngestorError>;
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthFilter {
    // Starting block
    #[prost(uint64, optional, tag = "1")]
    pub from_block: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub to_block: Option<u64>,
    #[prost(string, repeated, tag = "3")]
    #[serde(default)]
    pub addresses: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    #[serde(default)]
    pub topics: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthConfig {
    #[prost(message, optional, tag = "1")]
    pub filter: Option<EthFilter>,
    #[prost(string, tag = "2")]
    pub wss_url: String,
    #[prost(message, repeated, tag = "3")]
    #[serde(default)]
    pub contracts: Vec<EthContract>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct EthContract {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub address: String,
    #[prost(string, tag = "3")]
    pub abi: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct KafkaConfig {
    #[prost(string, tag = "1")]
    pub broker: String,
    #[prost(string, tag = "2")]
    pub topic: String,
    #[prost(string, optional, tag = "3")]
    pub schema_registry_url: Option<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct SnowflakeConfig {
    #[prost(string, tag = "1")]
    pub server: String,
    #[prost(string, tag = "2")]
    pub port: String,
    #[prost(string, tag = "3")]
    pub user: String,
    #[prost(string, tag = "4")]
    pub password: String,
    #[prost(string, tag = "5")]
    pub database: String,
    #[prost(string, tag = "6")]
    pub schema: String,
    #[prost(string, tag = "7")]
    pub warehouse: String,
    #[prost(string, optional, tag = "8")]
    pub driver: Option<String>,
}
