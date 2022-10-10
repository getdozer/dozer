use std::sync::Arc;
use atomic_counter::{AtomicCounter, ConsistentCounter};
use crate::connectors::storage::RocksStorage;

pub struct SeqNoResolver {
    storage_client: Arc<RocksStorage>,
    seq_no: Option<ConsistentCounter>
}

impl SeqNoResolver {
    pub fn new(storage_client: Arc<RocksStorage>) -> Self {
        Self {
            storage_client,
            seq_no: None
        }
    }

    pub fn init(&mut self) {
        let db = self.storage_client.get_db();
        let mut seq_iterator = db.raw_iterator();
        seq_iterator.seek_to_last();
        let mut initial_value = 0;

        if seq_iterator.valid() {
            initial_value = bincode::deserialize(seq_iterator.value().unwrap().as_ref()).unwrap();
        }

        self.seq_no = Some(ConsistentCounter::new(initial_value + 1));
    }

    pub fn get_next_seq_no(&mut self) -> usize {
        let counter = self.seq_no.take().unwrap();
        let position = counter.inc();
        self.seq_no = Some(counter);
        position
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use rocksdb::{DB, Options};
    use crate::connectors::seq_no_resolver::SeqNoResolver;
    use crate::connectors::storage::{RocksConfig, RocksStorage, Storage};

    #[test]
    fn test_new_sequence() {
        let storage_config = RocksConfig::default();
        DB::destroy(&Options::default(), &storage_config.path).unwrap();

        let lsn_storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));
        let mut seq_resolver = SeqNoResolver::new(Arc::clone(&lsn_storage_client));
        seq_resolver.init();

        let mut i = 1;

        while i < 10 {
            assert_eq!(seq_resolver.get_next_seq_no(), i);
            i = i + 1;
        }
    }

    #[test]
    fn test_continue_sequence() {
        let storage_config = RocksConfig::default();
        DB::destroy(&Options::default(), &storage_config.path).unwrap();

        let lsn_storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));
        let (key, value) = lsn_storage_client.map_ingestion_checkpoint_message(&(15 as usize), &1);
        lsn_storage_client.get_db().put(key, value).expect("Failed to insert");

        let mut seq_resolver = SeqNoResolver::new(Arc::clone(&lsn_storage_client));
        seq_resolver.init();

        let mut i = 16;

        while i < 25 {
            assert_eq!(seq_resolver.get_next_seq_no(), i);
            i = i + 1;
        }
    }
}