use dozer_types::types::*;
use rocksdb::{DBWithThreadMode, Options, ReadOptions, SingleThreaded, DB};
use std::sync::Arc;
use tempdir::TempDir;

pub struct TablePrefixes {
    pub seq_no_table: &'static str,
    pub seq_no_table_upper_bound: &'static str,
    pub commit_table: &'static str,
    pub commit_table_upper_bound: &'static str,
}

const TABLE_PREFIXES: TablePrefixes = TablePrefixes {
    seq_no_table: "0",
    seq_no_table_upper_bound: "1",
    commit_table: "1",
    commit_table_upper_bound: "2",
};

pub trait Storage<T> {
    fn new(storage_config: T) -> Self;
}

pub struct RocksStorage {
    _config: RocksConfig,
    db: Arc<DBWithThreadMode<SingleThreaded>>,
}
#[derive(Clone, Debug)]
pub struct RocksConfig {
    pub path: String,
}
impl RocksConfig {
    pub fn default() -> Self {
        let tmp_dir = TempDir::new("ingestion")
            .unwrap()
            .path()
            .to_str()
            .unwrap()
            .to_string();
        Self { path: tmp_dir }
    }

    pub fn _target() -> Self {
        Self {
            path: "target/ingestion-storage".to_string(),
        }
    }
}
impl Storage<RocksConfig> for RocksStorage {
    fn new(config: RocksConfig) -> RocksStorage {
        let db: Arc<DBWithThreadMode<SingleThreaded>> =
            Arc::new(DB::open_default(config.path.clone()).unwrap());
        RocksStorage {
            _config: config,
            db,
        }
    }
}

macro_rules! define_get_bounds_read_options_fn {
    ($name:ident, $lower_bound:expr, $upper_bound:expr) => {
        pub fn $name(&self) -> ReadOptions {
            let op_table_prefix =
                ($lower_bound.as_bytes().to_vec())..($upper_bound.as_bytes().to_vec());

            let mut ro = ReadOptions::default();
            ro.set_iterate_range(op_table_prefix);

            ro
        }
    };
}

impl RocksStorage {
    pub fn _get_estimate_key_count(&self) -> u64 {
        let db = Arc::clone(&self.db);
        let count: u64 = db
            .property_int_value("rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap();
        count
    }

    pub fn _destroy(&self) {
        let path = self._config.path.clone();
        let _ = DB::destroy(&Options::default(), path);
    }

    pub fn map_operation_event(&self, op: &OperationEvent) -> (Vec<u8>, Vec<u8>) {
        let key = self._get_operation_key(&op.seq_no);
        let encoded: Vec<u8> = bincode::serialize(op).unwrap();
        (key, encoded)
    }

    pub fn _get_operation_event(&self, id: u64) -> OperationEvent {
        let db = Arc::clone(&self.db);
        let key = self._get_operation_key(&id);
        let returned_bytes = db.get(key).unwrap().unwrap();
        let op: OperationEvent = bincode::deserialize(returned_bytes.as_ref()).unwrap();
        op
    }

    pub fn map_commit_message(
        &self,
        connection_id: &usize,
        seq_no: &usize,
        lsn: &u64,
    ) -> (Vec<u8>, Vec<u8>) {
        let key = format!("{}{:0>19}", TABLE_PREFIXES.commit_table, connection_id)
            .as_bytes()
            .to_owned();
        let encoded = bincode::serialize(&(seq_no, lsn)).unwrap();
        (key, encoded)
    }

    pub fn get_db(&self) -> Arc<DBWithThreadMode<SingleThreaded>> {
        Arc::clone(&self.db)
    }

    fn _get_operation_key(&self, seq_no: &u64) -> Vec<u8> {
        format!("{}{:0>19}", TABLE_PREFIXES.seq_no_table, seq_no)
            .as_bytes()
            .to_vec()
    }

    define_get_bounds_read_options_fn!(
        get_operations_table_read_options,
        TABLE_PREFIXES.seq_no_table,
        TABLE_PREFIXES.seq_no_table_upper_bound
    );
    define_get_bounds_read_options_fn!(
        get_commits_table_read_options,
        TABLE_PREFIXES.commit_table,
        TABLE_PREFIXES.commit_table_upper_bound
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::writer::{BatchedRocksDbWriter, Writer};

    #[test]
    fn serialize_and_deserialize() {
        let record = Record::new(
            Some(SchemaIdentifier { id: 1, version: 0 }),
            vec![Field::Int(1), Field::String("hello".to_string())],
        );
        let op = OperationEvent {
            operation: Operation::Insert { new: record },
            seq_no: 1,
        };

        let storage_config = RocksConfig::default();
        let storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));

        let (key, encoded) = storage_client.map_operation_event(&op);
        let mut writer = BatchedRocksDbWriter::new();
        writer.insert(key.as_ref(), encoded);
        writer.commit(&storage_client);

        let op2 = storage_client._get_operation_event(1);
        assert_eq!(op2.seq_no, op.seq_no);
    }
}
