use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use rdkafka::consumer::stream_consumer::StreamConsumer as RdkafkaStreamConsumer;
use rdkafka::consumer::DefaultConsumerContext;

pub trait StreamConsumer {
    fn run(
        &self,
        con: RdkafkaStreamConsumer<DefaultConsumerContext>,
        ingestor: &Ingestor,
    ) -> Result<(), ConnectorError>;
}
