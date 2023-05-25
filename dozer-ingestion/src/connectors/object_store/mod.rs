mod adapters;
mod connection;
pub mod connector;
mod csv;
mod delta;
mod helper;
mod parquet;
mod schema_helper;
pub mod schema_mapper;
mod table_reader;
pub(crate) mod table_watcher;
#[cfg(test)]
mod tests;
mod watcher;
