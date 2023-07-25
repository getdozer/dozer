use std::path::Path;

use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, LogLevel, Options, WriteOptions, DB};

use crate::{errors::StorageError, BorrowEncode, Encode, LmdbVal};
use dozer_types::borrow::IntoOwned;
use std::fmt::Debug;

pub struct RocksdbMap<K, V> {
    db: DB,
    write_options: WriteOptions,
    _key: std::marker::PhantomData<K>,
    _value: std::marker::PhantomData<V>,
}

impl<K, V> Debug for RocksdbMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksdbMap")
            .field("db", &self.db)
            .field("_key", &self._key)
            .field("_value", &self._value)
            .finish()
    }
}

const BLOCK_CACHE_SIZE: usize = 10 * 1024 * 1024 * 1024;
const WRITE_BUFFER_SIZE: usize = 1024 * 1024 * 1024;
const BLOCK_SIZE: usize = 4096;
const COMPACTION_MEMTABLE_BUDGET: usize = 1024 * 1024 * 1024 * 1024;
const MAX_WRITE_BUFFERS: i32 = 3;

impl<K: BorrowEncode, V: LmdbVal> RocksdbMap<K, V>
where
    for<'a> V::Borrowed<'a>: IntoOwned<V>,
{
    pub fn create(path: &Path) -> Result<Self, StorageError> {
        let mut opts = Options::default();

        opts.create_if_missing(true);
        opts.set_max_background_jobs(16);

        // Block cache
        let mut block_options = BlockBasedOptions::default();
        let cache = Cache::new_lru_cache(BLOCK_CACHE_SIZE);
        block_options.set_block_cache(&cache);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_top_level_index_and_filter(true);
        block_options.set_block_size(BLOCK_SIZE);
        block_options.set_bloom_filter(20.0, true);
        opts.set_block_based_table_factory(&block_options);

        opts.optimize_for_point_lookup(BLOCK_CACHE_SIZE as u64);
        opts.increase_parallelism(16);
        opts.set_optimize_filters_for_hits(true);

        // buffer
        opts.set_max_write_buffer_number(MAX_WRITE_BUFFERS);
        opts.set_write_buffer_size(WRITE_BUFFER_SIZE);
        // Compaction
        opts.optimize_universal_style_compaction(COMPACTION_MEMTABLE_BUDGET);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Universal);

        //Compression
        opts.set_compression_type(DBCompressionType::Lz4);

        // Log Options
        opts.set_log_level(LogLevel::Fatal);

        opts.set_recycle_log_file_num(16);

        opts.set_keep_log_file_num(1);

        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);

        let db = DB::open(&opts, path)?;
        Ok(Self {
            db,
            write_options,
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
        self.db.put_opt(key, value, &self.write_options)?;

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
