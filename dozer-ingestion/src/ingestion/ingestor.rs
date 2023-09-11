use crossbeam::channel::{bounded, Receiver};
use dozer_types::ingestion_types::{IngestionMessage, IngestorError, IngestorForwarder};
use dozer_types::log::warn;
use std::sync::Arc;
use std::time::Duration;

use super::IngestionConfig;

#[derive(Debug)]
pub struct ChannelForwarder {
    pub sender: crossbeam::channel::Sender<IngestionMessage>,
}

impl IngestorForwarder for ChannelForwarder {
    fn forward(&self, msg: IngestionMessage) -> Result<(), IngestorError> {
        self.sender
            .send(msg)
            .map_err(|e| IngestorError::ChannelError(Box::new(e)))
    }
}
#[derive(Debug)]
/// `IngestionIterator` is the receiver side of a spsc channel. The sender side is `Ingestor`.
pub struct IngestionIterator {
    pub rx: Receiver<IngestionMessage>,
}

impl Iterator for IngestionIterator {
    type Item = IngestionMessage;
    fn next(&mut self) -> Option<Self::Item> {
        let msg = self.rx.recv();
        match msg {
            Ok(msg) => Some(msg),
            Err(e) => {
                warn!("IngestionIterator: Error in receiving {:?}", e.to_string());
                None
            }
        }
    }
}
impl IngestionIterator {
    pub fn next_timeout(&mut self, timeout: Duration) -> Option<IngestionMessage> {
        let msg = self.rx.recv_timeout(timeout);
        match msg {
            Ok(msg) => Some(msg),
            Err(e) => {
                warn!("IngestionIterator: Error in receiving {:?}", e.to_string());
                None
            }
        }
    }
}

#[derive(Debug, Clone)]
/// `Ingestor` is the sender side of a spsc channel. The receiver side is `IngestionIterator`.
///
/// `IngestionMessage` is the message type that is sent over the channel.
pub struct Ingestor {
    pub sender: Arc<Box<dyn IngestorForwarder>>,
}

impl Ingestor {
    pub fn initialize_channel(config: IngestionConfig) -> (Ingestor, IngestionIterator) {
        let (tx, rx) = bounded(config.forwarder_channel_cap);
        let sender: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
        let ingestor = Self { sender };

        let iterator = IngestionIterator { rx };
        (ingestor, iterator)
    }

    pub fn handle_message(&self, message: IngestionMessage) -> Result<(), IngestorError> {
        self.sender.forward(message)
    }
}

#[cfg(test)]
mod tests {
    use super::{ChannelForwarder, Ingestor, IngestorForwarder};
    use crossbeam::channel::unbounded;
    use dozer_types::ingestion_types::IngestionMessage;
    use dozer_types::types::{Operation, Record};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_message_handle() {
        let (tx, rx) = unbounded();
        let sender: Arc<Box<dyn IngestorForwarder>> =
            Arc::new(Box::new(ChannelForwarder { sender: tx }));
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
            .unwrap();
        ingestor
            .handle_message(IngestionMessage::OperationEvent {
                table_index: 0,
                op: operation2.clone(),
                id: None,
            })
            .unwrap();
        ingestor
            .handle_message(IngestionMessage::SnapshottingDone)
            .unwrap();

        let expected_op_event_message = vec![operation, operation2].into_iter();

        for op in expected_op_event_message {
            let msg = rx.recv().unwrap();
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
