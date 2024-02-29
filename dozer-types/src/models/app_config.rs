use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// Pipeline buffer size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_buffer_size: Option<u32>,

    /// How many errors we can tolerate before bringing down the app.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_threshold: Option<u32>,

    /// The event hub's queue capacity. Events that are not processed will be dropped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_hub_capacity: Option<usize>,
}

pub fn default_app_buffer_size() -> u32 {
    20_000
}

pub fn default_error_threshold() -> u32 {
    0
}

pub fn default_event_hub_capacity() -> usize {
    100
}
