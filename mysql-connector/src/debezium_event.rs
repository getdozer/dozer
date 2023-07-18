// src/debezium_event.rs

use serde::{Deserialize, Serialize};

// Define the DebeziumEvent struct to represent the event payload
#[derive(Debug, Deserialize, Serialize)]
pub struct DebeziumEvent {
    // Customize the fields based on your actual Debezium event structure
    pub payload: serde_json::Value,
}

// Implement the DebeziumEvent deserialization from bytes
impl DebeziumEvent {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}