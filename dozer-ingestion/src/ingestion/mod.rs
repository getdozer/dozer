mod ingestor;

pub use ingestor::ChannelForwarder;
pub use ingestor::{IngestionIterator, Ingestor};

#[derive(Default)]
pub struct IngestionConfig {}
