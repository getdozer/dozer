use std::sync::Arc;

use dozer_shared::types::*;
use rocksdb::{DBWithThreadMode, SingleThreaded, DB};
pub trait Storage<T> {
    fn new(storage_config: T) -> Self;
}

#[derive(Clone, Debug)]
pub struct RocksStorage {
    _config: RocksConfig,
    db: Arc<DBWithThreadMode<SingleThreaded>>,
}
#[derive(Clone, Debug)]
pub struct RocksConfig {
    pub path: String,
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
    pub fn insert_operation_event(&self, op: &OperationEvent) {
        let db = Arc::clone(&self.db);
        let key = self._get_operation_key(op).to_owned();
        let key: &[u8] = key.as_ref();
        db.put(key, b"my value").unwrap();
    }

    pub fn insert_schema(&self, schema: &Schema) {
        let db = Arc::clone(&self.db);
        let key = self.get_schema_key(schema).to_owned();
        let key: &[u8] = key.as_ref();
        db.put(key, b"my value").unwrap();
    }

    fn _get_operation_key(&self, op: &OperationEvent) -> Vec<u8> {
        format!("operation_{}", op.id).as_bytes().to_vec()
    }

    fn get_schema_key(&self, schema: &Schema) -> Vec<u8> {
        format!("schema_{}", schema.id).as_bytes().to_vec()
    }
}
