use crate::connectors::storage::RocksStorage;
use atomic_counter::{AtomicCounter, ConsistentCounter};
use std::sync::Arc;

pub struct SeqNoResolver {
    storage_client: Arc<RocksStorage>,
    seq_no: Option<ConsistentCounter>,
}

impl SeqNoResolver {
    pub fn new(storage_client: Arc<RocksStorage>) -> Self {
        Self {
            storage_client,
            seq_no: None,
        }
    }

    pub fn init(&mut self) {
        let db = self.storage_client.get_db();
        let mut seq_iterator =
            db.raw_iterator_opt(self.storage_client.get_operations_table_read_options());
        seq_iterator.seek_to_last();
        let mut initial_value = 0;

        if seq_iterator.valid() {
            initial_value = bincode::deserialize(seq_iterator.value().unwrap()).unwrap();
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
    use crate::connectors::seq_no_resolver::SeqNoResolver;
    use crate::connectors::storage::{RocksConfig, RocksStorage, Storage};
    use dozer_types::types::{Operation, Record};
    use rocksdb::{Options, DB};
    use std::sync::Arc;

    fn get_event(seq_no: u64) -> dozer_types::types::OperationEvent {
        dozer_types::types::OperationEvent {
            seq_no,
            operation: Operation::Insert {
                new: Record::new(None, vec![]),
            },
        }
    }

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
            i += 1;
        }
    }

    #[test]
    fn test_continue_sequence() {
        let storage_config = RocksConfig::default();
        DB::destroy(&Options::default(), &storage_config.path).unwrap();

        let storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));
        let mut seq_no = 8;
        while seq_no < 14 {
            let (key, value) = storage_client.map_operation_event(&get_event(seq_no));
            storage_client
                .get_db()
                .put(key, value)
                .expect("Failed to insert");
            seq_no += 1;
        }

        let mut seq_resolver = SeqNoResolver::new(Arc::clone(&storage_client));
        seq_resolver.init();

        let mut i = 14;

        while i < 25 {
            assert_eq!(seq_resolver.get_next_seq_no(), i);
            i += 1;
        }
    }
}
