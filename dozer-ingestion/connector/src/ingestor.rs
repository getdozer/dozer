use dozer_types::models::ingestion_types::IngestionMessage;
use futures::Stream;
use std::{
    error::Error,
    fmt::Display,
    sync::{atomic::AtomicBool, Arc},
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
/// `IngestionIterator` is the receiver side of a spsc channel. The sender side is `Ingestor`.
pub struct IngestionIterator {
    pub receiver: Receiver<IngestionMessage>,
}

impl Iterator for IngestionIterator {
    type Item = IngestionMessage;
    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.blocking_recv()
    }
}

impl Stream for IngestionIterator {
    type Item = IngestionMessage;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl IngestionIterator {
    pub async fn next_timeout(&mut self, duration: Duration) -> Option<IngestionMessage> {
        timeout(duration, self.receiver.recv()).await.ok().flatten()
    }
}

#[derive(Debug, Clone)]
/// `Ingestor` is the sender side of a spsc channel. The receiver side is `IngestionIterator`.
///
/// `IngestionMessage` is the message type that is sent over the channel.
pub struct Ingestor {
    sender: Sender<IngestionMessage>,
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
        let ingestor = Self { sender };

        let iterator = IngestionIterator { receiver };
        (ingestor, iterator)
    }

    pub async fn handle_message(&self, message: IngestionMessage) -> Result<(), SendError> {
        self.sender.send(message).await.map_err(|_| SendError)
    }

    pub fn blocking_handle_message(&self, message: IngestionMessage) -> Result<(), SendError> {
        self.sender.blocking_send(message).map_err(|_| SendError)
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub async fn closed(&self) -> Arc<AtomicBool> {
        let bool = Arc::new(AtomicBool::new(false));
        let sender = self.sender.clone();
        let shared_bool = bool.clone();
        tokio::spawn(async move {
            sender.closed().await;
            shared_bool.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        bool
    }
}

#[cfg(test)]
mod tests {
    use super::Ingestor;
    use dozer_types::models::ingestion_types::{IngestionMessage, TransactionInfo};
    use dozer_types::types::{Operation, Record};

    #[tokio::test]
    async fn test_message_handle() {
        let (sender, mut rx) = tokio::sync::mpsc::channel(10);
        let ingestor = Ingestor { sender };

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

        for op in expected_op_event_message {
            let msg = rx.recv().await.unwrap();
            assert_eq!(
                IngestionMessage::OperationEvent {
                    table_index: 0,
                    op,
                    id: None
                },
                msg
            );
        }
    }
}
