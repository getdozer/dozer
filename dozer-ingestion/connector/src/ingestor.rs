use dozer_types::models::ingestion_types::IngestionMessage;
use std::{
    error::Error,
    fmt::Display,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};

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

#[derive(Debug)]
/// `IngestionIterator` is the receiver side of a mpsc channel. The sender side is `Ingestor`.
pub struct IngestionIterator {
    pub receiver: Receiver<(usize, IngestionMessage)>,
}

impl Iterator for IngestionIterator {
    type Item = IngestionMessage;
    fn next(&mut self) -> Option<Self::Item> {
        let (_idx, msg) = self.receiver.blocking_recv()?;
        Some(msg)
    }
}

impl IngestionIterator {
    pub async fn next_timeout(&mut self, duration: Duration) -> Option<IngestionMessage> {
        timeout(duration, self.receiver.recv())
            .await
            .ok()
            .flatten()
            .map(|(_id, msg)| msg)
    }
}

#[derive(Debug, Clone)]
/// `Ingestor` is the sender side of a spsc channel. The receiver side is `IngestionIterator`.
///
/// `IngestionMessage` is the message type that is sent over the channel.
pub struct Ingestor {
    msg_idx: Arc<AtomicUsize>,
    sender: Sender<(usize, IngestionMessage)>,
}

#[derive(Debug, Clone, Copy)]
pub struct SendError;

impl Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ingestor receiver dropped")
    }
}

impl Error for SendError {}

impl Ingestor {
    pub fn initialize_channel(config: IngestionConfig) -> (Ingestor, IngestionIterator) {
        let (sender, receiver) = channel(config.forwarder_channel_cap);
        let ingestor = Self {
            sender,
            msg_idx: Arc::new(0.into()),
        };

        let iterator = IngestionIterator { receiver };
        (ingestor, iterator)
    }

    pub async fn handle_message(&self, message: IngestionMessage) -> Result<usize, SendError> {
        let idx = self
            .msg_idx
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.sender
            .send((idx, message))
            .await
            .map_err(|_| SendError)?;
        Ok(idx)
    }

    pub fn blocking_handle_message(&self, message: IngestionMessage) -> Result<usize, SendError> {
        let idx = self
            .msg_idx
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.sender
            .blocking_send((idx, message))
            .map_err(|_| SendError)?;
        Ok(idx)
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::Ingestor;
    use dozer_types::models::ingestion_types::{IngestionMessage, TransactionInfo};
    use dozer_types::types::{Operation, Record};

    #[tokio::test]
    async fn test_message_handle() {
        let (ingestor, mut rx) = Ingestor::initialize_channel(crate::IngestionConfig {
            forwarder_channel_cap: 10,
        });

        // Expected seq no - 2
        let operation = Operation::Insert {
            new: Record::new(vec![]),
        };

        // Expected seq no - 3
        let operation2 = Operation::Insert {
            new: Record::new(vec![]),
        };

        ingestor
            .handle_message(IngestionMessage::OperationEvent {
                table_index: 0,
                op: operation.clone(),
                id: None,
            })
            .await
            .unwrap();
        ingestor
            .handle_message(IngestionMessage::OperationEvent {
                table_index: 0,
                op: operation2.clone(),
                id: None,
            })
            .await
            .unwrap();
        ingestor
            .handle_message(IngestionMessage::TransactionInfo(
                TransactionInfo::SnapshottingDone { id: None },
            ))
            .await
            .unwrap();

        let expected_op_event_message = vec![operation, operation2].into_iter();

        for (i, op) in expected_op_event_message.enumerate() {
            let msg = rx.receiver.recv().await.unwrap();
            assert_eq!(
                (
                    i,
                    IngestionMessage::OperationEvent {
                        table_index: 0,
                        op,
                        id: None
                    }
                ),
                msg
            );
        }
    }
}
