use crate::errors::ConnectorError;
use crate::errors::KafkaError::KafkaConnectionError;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::BorrowedMessage,
    util::Timeout,
    ClientConfig, Message, Offset,
};
use std::collections::HashMap;

pub struct StreamConsumerHelper;

pub type OffsetsMap = HashMap<String, (i32, i64)>; // key: topic, value: (partition, offset)

impl StreamConsumerHelper {
    pub async fn start(
        client_config: &ClientConfig,
        topics: &[&str],
    ) -> Result<BaseConsumer, ConnectorError> {
        Self::resume_impl(client_config, topics, None).await
    }

    pub async fn resume(
        client_config: &ClientConfig,
        topics: &[&str],
        offsets: &OffsetsMap,
    ) -> Result<BaseConsumer, ConnectorError> {
        Self::resume_impl(client_config, topics, Some(offsets)).await
    }

    pub fn update_offsets(offsets: &mut OffsetsMap, message: &BorrowedMessage<'_>) {
        let _ = offsets.insert(
            message.topic().into(),
            (message.partition(), message.offset()),
        );
    }

    async fn resume_impl(
        client_config: &ClientConfig,
        topics: &[&str],
        offsets: Option<&OffsetsMap>,
    ) -> Result<BaseConsumer, ConnectorError> {
        loop {
            match Self::try_resume(client_config, topics, offsets).await {
                Ok(con) => return Ok(con),
                Err(err) if is_network_failure(&err) => continue,
                Err(err) => Err(KafkaConnectionError(err))?,
            }
        }
    }

    async fn try_resume(
        client_config: &ClientConfig,
        topics: &[&str],
        offsets: Option<&OffsetsMap>,
    ) -> Result<BaseConsumer, rdkafka::error::KafkaError> {
        let con: BaseConsumer = client_config.create()?;
        con.subscribe(topics.iter().as_slice())?;

        if let Some(offsets) = offsets {
            for (topic, &(partition, offset)) in offsets.iter() {
                con.seek(topic, partition, Offset::Offset(offset), Timeout::Never)?;
            }
        }

        Ok(con)
    }
}

pub fn is_network_failure(err: &rdkafka::error::KafkaError) -> bool {
    use rdkafka::error::KafkaError::*;
    let error_code = match err {
        ConsumerCommit(error_code) => error_code,
        Flush(error_code) => error_code,
        Global(error_code) => error_code,
        GroupListFetch(error_code) => error_code,
        MessageConsumption(error_code) => error_code,
        MessageProduction(error_code) => error_code,
        MetadataFetch(error_code) => error_code,
        OffsetFetch(error_code) => error_code,
        Rebalance(error_code) => error_code,
        SetPartitionOffset(error_code) => error_code,
        StoreOffset(error_code) => error_code,
        MockCluster(error_code) => error_code,
        Transaction(rdkafka_err) => return rdkafka_err.is_retriable(),
        _ => todo!(),
    };
    use rdkafka::types::RDKafkaErrorCode::*;
    matches!(
        error_code,
        Fail | BrokerTransportFailure
            | Resolve
            | MessageTimedOut
            | AllBrokersDown
            | OperationTimedOut
            | TimedOutQueue
            | Retry
            | PollExceeded
            | RequestTimedOut
            | NetworkException
    )
}
