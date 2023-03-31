use std::path::Path;

use dozer_cache::{
    cache::{RoCache, RwCacheManager},
    errors::CacheError,
};
use futures_util::StreamExt;

mod log_reader;

pub fn build_cache(
    cache_manager: &dyn RwCacheManager,
    path: &str,
) -> Result<Box<dyn RoCache>, CacheError> {
    let (mut log_reader, schema) = log_reader::LogReader::new(path);
    let secondary_indexes = todo!("Generate default index definitions");
    let rw_cache = cache_manager.create_cache(
        schema,
        secondary_indexes,
        todo!("conflict resolution from config"),
    )?;
    let ro_cache = cache_manager
        .open_ro_cache(rw_cache.name())?
        .expect("Cache just created");
    tokio::spawn(async move {
        while let Some(executor_operation) = log_reader.next().await {
            todo!("Insert operation into cache");
        }
    });
    Ok(ro_cache)
}
