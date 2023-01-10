use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::parking_lot::RwLock;
use kafka::consumer::Consumer;
use std::sync::Arc;

pub trait StreamConsumer {
    fn run(&self, con: Consumer, ingestor: Arc<RwLock<Ingestor>>) -> Result<(), ConnectorError>;
}
