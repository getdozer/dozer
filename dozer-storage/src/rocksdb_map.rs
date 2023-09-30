use std::path::Path;

use rocksdb::{BlockBasedOptions, Cache, Options, DB};

use dozer_types::borrow::IntoOwned;
use dozer_types::models::app_config::RocksdbConfig;

use crate::{errors::StorageError, BorrowEncode, Encode, LmdbVal};

#[derive(Debug)]
pub struct RocksdbMap<K, V> {
    db: DB,
    _key: std::marker::PhantomData<K>,
    _value: std::marker::PhantomData<V>,
}

impl<K: BorrowEncode, V: LmdbVal> RocksdbMap<K, V>
where
    for<'a> V::Borrowed<'a>: IntoOwned<V>,
{
    pub fn create(path: &Path, config: RocksdbConfig) -> Result<Self, StorageError> {
        let mut options = Options::default();
        options.create_if_missing(true);

        let mut block_options = BlockBasedOptions::default();
        if let Some(block_cache_size) = config.block_cache_size {
            let cache = Cache::new_lru_cache(block_cache_size);
            block_options.set_block_cache(&cache);
        }

        if let Some(max_write_buffer_number) = config.max_write_buffer_number {
            options.set_max_write_buffer_number(max_write_buffer_number);
        }

        if let Some(write_buffer_size) = config.write_buffer_size {
            options.set_write_buffer_size(write_buffer_size);
        }

        options.set_block_based_table_factory(&block_options);

        let db = DB::open(&options, path)?;
        Ok(Self {
            db,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        })
    }

    pub fn count(&self) -> Result<usize, StorageError> {
        Ok(self
            .db
            .property_int_value("rocksdb.estimate-num-keys")?
            .expect("rocksdb.estimate-num-keys") as usize)
    }

    pub fn get(&self, key: K::Encode<'_>) -> Result<Option<V>, StorageError> {
        let key = key.encode()?;
        let value = self.db.get_pinned(key)?;
        if let Some(value) = value {
            let value = V::decode(&value)?;
            Ok(Some(value.into_owned()))
        } else {
            Ok(None)
        }
    }

    pub fn contains(&self, key: K::Encode<'_>) -> Result<bool, StorageError> {
        let key = key.encode()?;
        let value = self.db.get_pinned(key)?;
        Ok(value.is_some())
    }

    pub fn insert(&self, key: K::Encode<'_>, value: V::Encode<'_>) -> Result<(), StorageError> {
        let key = key.encode()?;
        let value = value.encode()?;
        self.db.put(key, value)?;
        Ok(())
    }

    pub fn remove(&self, key: K::Encode<'_>) -> Result<(), StorageError> {
        let key = key.encode()?;
        self.db.delete(key)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().map_err(Into::into)
    }
}
