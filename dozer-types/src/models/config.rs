use super::{
    api_config::ApiConfig, app_config::AppConfig, connection::Connection, equal_default,
    flags::Flags, lambda_config::LambdaConfig, sink::Sink, source::Source,
    telemetry::TelemetryConfig,
};
use crate::constants::DEFAULT_HOME_DIR;
use crate::models::udf_config::UdfConfig;
use prettytable::Table as PrettyTable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default, JsonSchema)]
#[serde(deny_unknown_fields)]
/// The configuration for the app
pub struct Config {
    pub version: u32,

    /// Unique application Id
    #[serde(default)]
    pub id: String,

    /// Unique application Id
    #[serde(default)]
    pub company_id: String,

    /// name of the app
    pub app_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    ///directory for all process; Default: ./.dozer
    pub home_dir: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// connections to databases: Eg: Postgres, Snowflake, etc
    pub connections: Vec<Connection>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// sources to ingest data related to particular connection
    pub sources: Vec<Source>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// sinks to output data to
    pub sinks: Vec<Sink>,

    #[serde(default, skip_serializing_if = "equal_default")]
    /// Api server config related: port, host, etc
    pub api: ApiConfig,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// transformations to apply to source data in SQL format as multiple queries
    pub sql: Option<String>,

    #[serde(default, skip_serializing_if = "equal_default")]
    /// flags to enable/disable features
    pub flags: Flags,

    #[serde(default, skip_serializing_if = "equal_default")]
    /// App runtime config: behaviour of pipeline and log
    pub app: AppConfig,

    #[serde(default, skip_serializing_if = "equal_default")]
    /// Instrument using Dozer
    pub telemetry: TelemetryConfig,

    /// UDF specific configuration (eg. !Onnx)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub udfs: Vec<UdfConfig>,

    /// Lambda functions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lambdas: Vec<LambdaConfig>,
}

pub fn default_home_dir() -> String {
    DEFAULT_HOME_DIR.to_owned()
}

impl Config {
    pub fn convert_to_table(&self) -> PrettyTable {
        let mut table = table!();

        table.add_row(row!["name", self.app_name]);
        table.add_row(row![
            "connectors",
            self.connections
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<String>>()
                .join(", ")
        ]);
        if !self.sinks.is_empty() {
            table.add_row(row!["endpoints", table!()]);
        }

        table
    }
}
