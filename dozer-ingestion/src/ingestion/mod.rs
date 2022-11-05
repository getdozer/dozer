use std::sync::{Arc, Mutex};

use seq_no_resolver::SeqNoResolver;
use storage::RocksStorage;

mod ingestor;
mod seq_no_resolver;
mod storage;
mod writer;

pub use ingestor::ChannelForwarder;
pub use ingestor::Ingestor;
pub struct IngestionConfig {
    pub storage_client: Arc<RocksStorage>,
    pub seq_resolver: Arc<Mutex<SeqNoResolver>>,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        let storage_client = Arc::new(RocksStorage::default());
        Self {
            storage_client: storage_client.clone(),
            seq_resolver: Arc::new(Mutex::new(get_seq_resolver(storage_client))),
        }
    }
}
pub fn get_seq_resolver(storage_client: Arc<RocksStorage>) -> SeqNoResolver {
    let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
    seq_resolver.init();
    seq_resolver
}
