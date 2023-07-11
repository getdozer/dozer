mod telemetry;
pub use telemetry::{init_telemetry, init_telemetry_closure, shutdown_telemetry};
mod exporter;
mod helper;
pub const API_LATENCY_HISTOGRAM_NAME: &str = "api_latency";
pub const DATA_LATENCY_HISTOGRAM_NAME: &str = "data_latency";
pub const PIPELINE_LATENCY_HISTOGRAM_NAME: &str = "pipeline_latency";
pub const LATENCY_HISTOGRAME_NAME: &str = "product.latency";

pub const API_REQUEST_COUNTER_NAME: &str = "api_requests";
pub const CACHE_OPERATION_COUNTER_NAME: &str = "cache_operation";
pub const UNSATISFIED_JOINS_COUNTER_NAME: &str = "product.unsatisfied_joins";
pub const IN_OPS_COUNTER_NAME: &str = "product.in_ops";
pub const OUT_OPS_COUNTER_NAME: &str = "product.out_ops";

pub const LEFT_LOOKUP_SIZE_GAUGE_NAME: &str = "product.left_lookup_size";
pub const RIGHT_LOOKUP_SIZE_GAUGE_NAME: &str = "product.right_lookup_size";
