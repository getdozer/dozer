mod by_id;
mod dynamic_codec;
mod dynamic_service;
mod list;
mod on_delete;
mod on_insert;
mod on_schema_change;
mod on_update;
mod query;
pub mod util;
pub use dynamic_service::DynamicService;

#[cfg(test)]
mod tests;
