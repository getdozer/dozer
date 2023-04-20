use crate::ingestion_types::{
    DeltaLakeConfig, EthConfig, GrpcConfig, KafkaConfig, LocalStorage, S3Storage, SnowflakeConfig,
};
use serde::{Deserialize, Serialize};

use prettytable::Table;

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct Connection {
    #[prost(oneof = "ConnectionConfig", tags = "1,2,3,4,5,6,7,8")]
    /// authentication config - depends on db_type
    pub config: Option<ConnectionConfig>,
    #[prost(string, tag = "9")]
    pub name: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
pub struct PostgresConfig {
    #[prost(string, tag = "1")]
    pub user: String,
    #[prost(string, tag = "2")]
    pub password: String,
    #[prost(string, tag = "3")]
    pub host: String,
    #[prost(uint32, tag = "4")]
    pub port: u32,
    #[prost(bool, tag = "5")]
    pub ssl_verify: bool,
    #[prost(string, tag = "6")]
    pub database: String,
}

impl PostgresConfig {
    pub fn convert_to_table(&self) -> Table {
        table!(
            ["user", self.user.as_str()],
            ["password", "*************"],
            ["host", self.host],
            ["port", self.port],
            ["ssl_verify", self.ssl_verify],
            ["database", self.database]
        )
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
pub enum ConnectionConfig {
    #[prost(message, tag = "1")]
    /// In yaml, present as tag: `!Postgres`
    Postgres(PostgresConfig),
    #[prost(message, tag = "2")]
    /// In yaml, present as tag: `!Ethereum`
    Ethereum(EthConfig),
    /// In yaml, present as tag: `!Grpc`
    #[prost(message, tag = "3")]
    Grpc(GrpcConfig),
    #[prost(message, tag = "4")]
    /// In yaml, present as tag: `!Snowflake`
    Snowflake(SnowflakeConfig),
    #[prost(message, tag = "5")]
    /// In yaml, present as tag: `!Kafka`
    Kafka(KafkaConfig),
    #[prost(message, tag = "6")]
    /// In yaml, present as tag: `!ObjectStore`
    S3Storage(S3Storage),
    #[prost(message, tag = "7")]
    /// In yaml, present as tag: `!ObjectStore`
    LocalStorage(LocalStorage),
    #[prost(message, tag = "8")]
    /// In yaml, present as tag" `!DeltaLake`
    DeltaLake(DeltaLakeConfig),
}
