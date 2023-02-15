use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use kafka::consumer::Consumer;

pub trait StreamConsumer {
    fn run(&self, con: Consumer, ingestor: Ingestor) -> Result<(), ConnectorError>;
}
