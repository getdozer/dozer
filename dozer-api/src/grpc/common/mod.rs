mod service;
pub use service::CommonService;

#[cfg(test)]
mod tests;

pub const SERVICE_NAME: &str = "common";
