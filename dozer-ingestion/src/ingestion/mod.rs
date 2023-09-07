mod ingestor;

pub use ingestor::ChannelForwarder;
pub use ingestor::{IngestionIterator, Ingestor};

#[derive(Debug, Clone)]
pub struct IngestionConfig {
    forwarder_channel_cap: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            forwarder_channel_cap: 100000,
        }
    }
}
