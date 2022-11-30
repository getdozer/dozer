#[cfg(feature = "snowflake")]
pub mod connection;
pub mod connector;
#[cfg(feature = "snowflake")]
mod schema_helper;
#[cfg(feature = "snowflake")]
pub mod snapshotter;
#[cfg(feature = "snowflake")]
pub mod stream_consumer;
#[cfg(feature = "snowflake")]
pub mod test_utils;

#[cfg(test)]
#[cfg(feature = "snowflake")]
mod tests;
