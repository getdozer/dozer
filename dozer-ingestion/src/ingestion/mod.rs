mod ingestor;

pub use ingestor::ChannelForwarder;
pub use ingestor::{IngestionIterator, Ingestor};

pub struct IngestionConfig {}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {}
    }
}
