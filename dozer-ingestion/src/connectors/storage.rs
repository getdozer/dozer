use std::sync::Arc;

use dozer_types::types::*;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
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

    pub fn map_schema(&self, schema: &Schema) -> (Vec<u8>, Vec<u8>) {
        let key = self.get_schema_key(schema).to_owned();
        let encoded: Vec<u8> = bincode::serialize(schema).unwrap();
        (key, encoded)
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
        format!("operation_{}", op.id).as_bytes().to_vec()
    }

    fn get_schema_key(&self, schema: &Schema) -> Vec<u8> {
        format!("schema_{}", schema.id).as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::writer::{BatchedRocksDbWriter, Writer};

    #[test]
    fn serialize_and_deserialize() {
        let record = Record::new(1, vec![Field::Int(1), Field::String("hello".to_string())]);
        let op = OperationEvent {
            operation: Operation::Insert {
                table_name: "actor".to_string(),
                new: record,
            },
            id: 1,
        };

        let storage_config = RocksConfig {
            path: "./target/ingestion-storage-test".to_string(),
        };
        let storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));

        let (key, encoded) = storage_client.map_operation_event(&op);
        let mut writer = BatchedRocksDbWriter::new();
        writer.insert(key.as_ref(), encoded);
        writer.commit(&storage_client);

        let op2 = storage_client.get_operation_event(1);
        assert_eq!(op2.id, op.id);
    }
}
