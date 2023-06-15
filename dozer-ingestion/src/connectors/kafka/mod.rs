pub mod connector;
pub mod debezium;
pub mod no_schema_registry_basic;
pub mod schema_registry_basic;
pub mod stream_consumer;
pub mod stream_consumer_basic;
#[cfg(any(test, feature = "debezium_bench"))]
pub mod test_utils;
#[cfg(test)]
mod tests;
