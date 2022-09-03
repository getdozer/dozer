use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectionType {
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "snowflake")]
    Snowflake,
    #[serde(rename = "databricks")]
    Databricks,
}

impl std::fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}",
            match self {
                ConnectionType::Postgres => "postgres",
                ConnectionType::Snowflake => "snowflake",
                ConnectionType::Databricks => "databricks",
            }
        )
    }
}

    