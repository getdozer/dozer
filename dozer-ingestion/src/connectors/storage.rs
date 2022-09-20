use std::borrow::Borrow;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use postgres_protocol::Lsn;

use dozer_shared::types::*;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB, WriteBatch};
pub trait Storage<T> {
    fn new(storage_config: T) -> Self;
}

pub struct RocksStorage {
    _config: RocksConfig,
    db: Arc<DBWithThreadMode<SingleThreaded>>,
    first_lsn: u64,
    end_lsn: u64,
    batch: Arc<Box<WriteBatch>>,
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
            first_lsn: 0,
            end_lsn: 0,
            batch: Arc::new(Box::new(WriteBatch::default()))
        }
    }
}

impl RocksStorage {
    fn get_batch(&mut self) -> &Arc<Box<WriteBatch>> {
        // if let None = self.batch {
            self.batch = Arc::new(Box::new(WriteBatch::default()));
        // }

        &self.batch
    }

    pub fn insert_to_batch(&self, key: &[u8], encoded: Vec<u8>) {
        // println!("To batch: {:?}", key);
        // let mut batch = Arc::clone(&self.batch).as_ref().lock().unwrap();
        self.batch.as_ref().put(key, encoded);
    }

    pub fn flush_batch(&mut self) {
        let db = Arc::clone(&self.db);
        let batch = Arc::clone(&self.batch);
        db.write(batch).unwrap();
    }

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

    pub fn insert_schema(&self, schema: &Schema) {
        let key = self.get_schema_key(schema).to_owned();
        let key: &[u8] = key.as_ref();
        let encoded: Vec<u8> = bincode::serialize(schema).unwrap();
        self.insert_to_batch(key, encoded);
    }

    pub fn insert_operation_event(&self, op: &OperationEvent) {
        let db = Arc::clone(&self.db);
        let key = self._get_operation_key(op).to_owned();
        let key: &[u8] = key.as_ref();
        let encoded: Vec<u8> = bincode::serialize(op).unwrap();
        self.insert_to_batch(key, encoded);
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
        let mut storage_client: Arc<RocksStorage> = Arc::new(Storage::new(storage_config));
        storage_client.insert_operation_event(&op);

        let op2 = storage_client.get_operation_event(1);
        assert_eq!(op2.id, op.id);
    }
}
