use dozer_types::types::*;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
use std::sync::Arc;
use tempdir::TempDir;
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
            path: "target/schema-registry".to_string(),
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

impl RocksStorage {
    pub fn get_estimate_key_count(&self) -> u64 {
        let db = Arc::clone(&self.db);
        let count: u64 = db
            .property_int_value("rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap();
        count
    }

    pub fn destroy(&self) {
        let path = self._config.path.clone();
        let _ = DB::destroy(&Options::default(), path);
    }

    pub fn map_operation_event(&self, op: &OperationEvent) -> (Vec<u8>, Vec<u8>) {
        let key = self._get_operation_key(op).to_owned();
        let encoded: Vec<u8> = bincode::serialize(op).unwrap();
        (key, encoded)
    }

    pub fn get_operation_event(&self, id: i32) -> OperationEvent {
        let db = Arc::clone(&self.db);
        let key = format!("operation_{}", id).as_bytes().to_owned();
        let returned_bytes = db.get(key).unwrap().unwrap();
        let op: OperationEvent = bincode::deserialize(returned_bytes.as_ref()).unwrap();
        op
    }

    pub fn get_db(&self) -> Arc<DBWithThreadMode<SingleThreaded>> {
        Arc::clone(&self.db)
    }

    fn _get_operation_key(&self, op: &OperationEvent) -> Vec<u8> {
        format!("operation_{}", op.seq_no).as_bytes().to_vec()
    }
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

        let op2 = storage_client.get_operation_event(1);
        assert_eq!(op2.seq_no, op.seq_no);
    }
}
