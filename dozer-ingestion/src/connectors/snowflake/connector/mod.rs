#[cfg(not(feature = "snowflake"))]
mod placeholder;
#[cfg(not(feature = "snowflake"))]
pub use placeholder::PlaceHolderSnowflakeConnector as SnowflakeConnector;

#[cfg(feature = "snowflake")]
mod snowflake;
#[cfg(feature = "snowflake")]
pub use snowflake::SnowflakeConnector;
