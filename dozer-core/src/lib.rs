pub mod app;
pub mod appsource;
mod builder_dag;
pub mod channels;
mod dag_impl;
pub use dag_impl::*;
pub mod dag_schemas;
mod error_manager;
pub mod errors;
pub mod executor;
pub mod executor_operation;
pub mod forwarder;
mod hash_map_to_vec;
pub mod node;
pub mod record_store;
pub mod shutdown;
pub use tokio;

#[cfg(test)]
mod tests;

pub mod test_utils {
    use std::sync::atomic::AtomicUsize;

    use dozer_types::{models::ingestion_types::IngestionMessage, types::PortHandle};
    use tokio::sync::mpsc::Sender;

    use crate::node::SourceMessage;

    pub struct CountingSender {
        count: AtomicUsize,
        sender: Sender<SourceMessage>,
    }

    impl CountingSender {
        pub fn new(sender: Sender<SourceMessage>) -> Self {
            Self {
                count: 0.into(),
                sender,
            }
        }

        pub async fn send(
            &self,
            port: PortHandle,
            message: IngestionMessage,
        ) -> Result<(), tokio::sync::mpsc::error::SendError<SourceMessage>> {
            let idx = self.count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            self.sender
                .send(SourceMessage {
                    id: idx,
                    port,
                    message,
                })
                .await?;
            Ok(())
        }
    }
}

pub use daggy::{self, petgraph};
pub use dozer_types::{epoch, event};
