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
        let encoded: Vec<u8> = bincode::serialize(op).unwrap();
        db.put(key, encoded).unwrap();
    }

    pub fn insert_schema(&self, schema: &Schema) {
        let db = Arc::clone(&self.db);
        let key = self.get_schema_key(schema).to_owned();
        let key: &[u8] = key.as_ref();
        let encoded: Vec<u8> = bincode::serialize(schema).unwrap();

        db.put(key, encoded).unwrap();
    }

    pub fn get_operation_event(&self, id: i32) -> OperationEvent {
        let db = Arc::clone(&self.db);
        let key = format!("operation_{}", id).as_bytes().to_owned();
        let returned_bytes = db.get(key).unwrap().unwrap();
        let op: OperationEvent = bincode::deserialize(returned_bytes.as_ref()).unwrap();
        op
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
            path: "./db/test".to_string(),
        };
        let storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));
        storage_client.insert_operation_event(&op);

        let op2 = storage_client.get_operation_event(1);
        assert_eq!(op2.id, op.id);
    }
}
