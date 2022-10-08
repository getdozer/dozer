use std::sync::Arc;

use anyhow::Context;
use dozer_types::types::*;
use rocksdb::{DBWithThreadMode, Options, SingleThreaded, DB};
use tempdir::TempDir;
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
    pub fn _default() -> Self {
        let tmp_dir = TempDir::new("schema-registry")
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

    pub fn insert_schema(&self, schema: &Schema) -> anyhow::Result<()> {
        let db = Arc::clone(&self.db);
        let key = get_schema_key(&schema.identifier.to_owned().context("schema_id expected")?);
        let key: &[u8] = key.as_ref();
        let encoded: Vec<u8> = bincode::serialize(schema)?;
        db.put(key, encoded)?;
        Ok(())
    }

    pub fn get_schema(&self, schema_id: &SchemaIdentifier) -> anyhow::Result<Schema> {
        let db = Arc::clone(&self.db);
        let key = get_schema_key(schema_id);

        let returned_bytes = db.get(key)?.context("schema not found")?;
        let schema: Schema = bincode::deserialize(returned_bytes.as_ref())?;
        Ok(schema)
    }
}

pub fn get_schema_key(schema_id: &SchemaIdentifier) -> Vec<u8> {
    [
        "sc".as_bytes(),
        &schema_id.id.to_be_bytes().to_vec(),
        &schema_id.version.to_be_bytes().to_vec(),
    ]
    .join("#".as_bytes())
}
