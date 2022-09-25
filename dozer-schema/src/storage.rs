use std::sync::Arc;

use dozer_types::types::*;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
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
impl RocksConfig {
    pub fn default() -> Self {
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

    pub fn insert_schema(&self, schema: &Schema) {
        let db = Arc::clone(&self.db);
        let key = self.get_schema_key(schema).to_owned();
        let key: &[u8] = key.as_ref();
        println!("{:?}", schema);
        let encoded: Vec<u8> = bincode::serialize(schema).unwrap();
        db.put(key, encoded).unwrap();
    }

    pub fn get_schema(&self, schema_id: u32) -> Schema {
        let db = Arc::clone(&self.db);
        let key = format!("schema_{}", schema_id).as_bytes().to_owned();

        let returned_bytes = db.get(key).unwrap().unwrap();
        let schema: Schema = bincode::deserialize(returned_bytes.as_ref()).unwrap();
        schema
    }

    fn get_schema_key(&self, schema: &Schema) -> Vec<u8> {
        format!("schema_{}", schema.id).as_bytes().to_vec()
    }
}
