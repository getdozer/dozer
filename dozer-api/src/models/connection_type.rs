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

impl std::str::FromStr for ConnectionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "postgres" => Ok(ConnectionType::Postgres),
            "snowflake" => Ok(ConnectionType::Snowflake),
            "databricks" => Ok(ConnectionType::Databricks),
            _ => Err(format!("'{}' is not a valid value for ConnectionType", s)),
        }
    }
}

    