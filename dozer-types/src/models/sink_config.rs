use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub enum SinkConfig {
    Snowflake(Snowflake),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Snowflake {
    pub connection: snowflake::ConnectionParameters,
    pub endpoint: String,
    pub destination: snowflake::Destination,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<snowflake::Options>,
}

pub mod snowflake {
    use std::time::Duration;

    use crate::helper::{deserialize_duration_secs_f64, f64_schema, serialize_duration_secs_f64};
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct ConnectionParameters {
        pub server: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub port: Option<String>,

        pub user: String,
        pub password: String,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub role: Option<String>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub driver: Option<String>,

        pub warehouse: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct Destination {
        pub database: String,
        pub schema: String,
        pub table: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct Options {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub batch_size: Option<usize>,

        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "deserialize_duration_secs_f64",
            serialize_with = "serialize_duration_secs_f64"
        )]
        #[schemars(schema_with = "f64_schema")]
        pub batch_interval_seconds: Option<Duration>,

        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub suspend_warehouse_after_each_batch: Option<bool>,
    }

    impl Default for Options {
        fn default() -> Self {
            Self {
                batch_size: Some(default_batch_size()),
                batch_interval_seconds: Some(default_batch_interval()),
                suspend_warehouse_after_each_batch: Some(default_suspend_warehouse()),
            }
        }
    }

    pub fn default_batch_size() -> usize {
        1000000
    }

    pub fn default_batch_interval() -> Duration {
        Duration::from_secs(1)
    }

    pub fn default_suspend_warehouse() -> bool {
        true
    }
}
