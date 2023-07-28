use std::path::Path;

use rocksdb::{
    BlockBasedOptions, Cache, DBCompressionType, DataBlockIndexType, LogLevel, Options,
    PlainTableFactoryOptions, WriteOptions, DB,
};

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
const MAX_WRITE_BUFFERS: i32 = 10;

impl<K: BorrowEncode, V: LmdbVal> RocksdbMap<K, V>
where
    for<'a> V::Borrowed<'a>: IntoOwned<V>,
{
    pub fn get_default_options() -> Options {
        let mut opts = Options::default();

        opts.create_if_missing(true);
        opts.set_max_background_jobs(10);

        opts.optimize_for_point_lookup(BLOCK_CACHE_SIZE as u64);
        opts.increase_parallelism(10);
        opts.set_optimize_filters_for_hits(true);

        // At what point does level 0 trigger compaction
        // opts.set_level_zero_file_num_compaction_trigger(10);

        // Disable compaction
        // opts.set_disable_auto_compactions(true);

        opts.set_memtable_whole_key_filtering(true);

        // buffer
        opts.set_max_write_buffer_number(MAX_WRITE_BUFFERS);
        opts.set_write_buffer_size(WRITE_BUFFER_SIZE);
        // Compaction
        // opts.optimize_universal_style_compaction(COMPACTION_MEMTABLE_BUDGET);
        // opts.set_compaction_style(rocksdb::DBCompactionStyle::Universal);

        //Compression
        opts.set_compression_type(DBCompressionType::Lz4);

        // Log Options
        // opts.set_log_level(LogLevel::Fatal);

        // opts.set_recycle_log_file_num(16);

        // opts.set_keep_log_file_num(1);

        opts
    }
    pub fn get_plain_options() -> Options {
        let mut opts = Self::get_default_options();
        let plain_opts = PlainTableFactoryOptions {
            user_key_length: 0,
            bloom_bits_per_key: 20,
            hash_table_ratio: 0.75,
            index_sparseness: 0,
        };
        opts.set_plain_table_factory(&plain_opts);
        opts
    }

    pub fn get_block_options() -> Options {
        let mut opts = Self::get_default_options();

        // Block cache
        let mut block_options = BlockBasedOptions::default();
        // let cache = Cache::new_lru_cache(BLOCK_CACHE_SIZE);

        let cache = Cache::new_hyper_clock_cache(BLOCK_CACHE_SIZE, 1);
        block_options.set_block_cache(&cache);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_top_level_index_and_filter(true);
        // block_options.set_block_size(BLOCK_SIZE);
        block_options.set_bloom_filter(20.0, true);

        //Index options
        block_options.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);

        block_options.set_format_version(5);

        opts.set_block_based_table_factory(&block_options);

        opts
    }
    pub fn create(path: &Path) -> Result<Self, StorageError> {
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(true);

        // let opts = Self::get_plain_options();
        let opts = Self::get_block_options();

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
