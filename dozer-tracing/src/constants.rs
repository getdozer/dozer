// Meter Name
pub const DOZER_METER_NAME: &str = "dozer.meter";

// Metrics
pub const SINK_OPERATION_COUNTER_NAME: &str = "sink_operation";
pub const PIPELINE_LATENCY_GAUGE_NAME: &str = "pipeline_latency";

pub const SOURCE_OPERATION_COUNTER_NAME: &str = "source_operation";

//  Labels
pub const OPERATION_TYPE_LABEL: &str = "operation_type";
pub const TABLE_LABEL: &str = "table";
pub const CONNECTION_LABEL: &str = "connection";

// Traces
pub const CONNECTOR_EVENTS: &str = "connector_events";

pub enum ConnectorEntityType {
    Connector,
    Sink,
}

impl std::fmt::Display for ConnectorEntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConnectorEntityType::Connector => write!(f, "connector"),
            ConnectorEntityType::Sink => write!(f, "sink"),
        }
    }
}
