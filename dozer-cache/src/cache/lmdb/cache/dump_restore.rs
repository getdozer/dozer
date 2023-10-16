use std::sync::Arc;

use dozer_storage::{
    errors::StorageError,
    generator::FutureGeneratorContext,
    lmdb::{RoTransaction, Transaction},
    DumpItem, LmdbEnvironment,
};
use dozer_types::{log::info, parking_lot::Mutex};
use tokio::io::AsyncRead;

use crate::{
    cache::{lmdb::indexing::IndexingThreadPool, CacheWriteOptions, RoCache},
    errors::CacheError,
};

use super::{
    main_environment, secondary_environment, secondary_environment_name, CacheOptions, LmdbCache,
    LmdbRwCache, MainEnvironment, SecondaryEnvironment,
};

pub struct DumpTransaction<T: Transaction> {
    main_txn: T,
    main_env_metadata: Option<u64>,
    secondary_txns: Vec<T>,
    secondary_metadata: Vec<u64>,
}

impl<T: Transaction> DumpTransaction<T> {
    pub fn main_env_metadata(&self) -> Option<u64> {
        self.main_env_metadata
    }

    pub fn secondary_metadata(&self) -> &[u64] {
        &self.secondary_metadata
    }
}

pub fn begin_dump_txn<C: LmdbCache>(
    cache: &C,
) -> Result<DumpTransaction<RoTransaction>, CacheError> {
    let main_env = cache.main_env();
    let main_txn = main_env.begin_txn()?;
    let main_env_metadata = main_env.metadata_with_txn(&main_txn)?;

    let mut secondary_txns = vec![];
    let mut secondary_metadata = vec![];
    for index in 0..cache.get_schema().1.len() {
        let secondary_env = cache.secondary_env(index);
        let txn = secondary_env.begin_txn()?;
        let metadata = secondary_env.next_operation_id(&txn)?;
        secondary_txns.push(txn);
        secondary_metadata.push(metadata);
    }

    Ok(DumpTransaction {
        main_txn,
        main_env_metadata,
        secondary_txns,
        secondary_metadata,
    })
}

pub async fn dump<'txn, T: Transaction, C: LmdbCache>(
    cache: &C,
    txn: &'txn DumpTransaction<T>,
    context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
) -> Result<(), ()> {
    main_environment::dump_restore::dump(cache.main_env(), &txn.main_txn, context).await?;

    for index in 0..cache.get_schema().1.len() {
        let secondary_env = cache.secondary_env(index);
        let txn = &txn.secondary_txns[index];
        secondary_environment::dump_restore::dump(secondary_env, txn, context).await?;
    }

    Ok(())
}

pub async fn restore(
    options: &CacheOptions,
    write_options: CacheWriteOptions,
    indexing_thread_pool: Arc<Mutex<IndexingThreadPool>>,
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<LmdbRwCache, CacheError> {
    info!("Restoring cache with options {options:?}");
    let rw_main_env =
        main_environment::dump_restore::restore(options, write_options, reader).await?;

    let options = CacheOptions {
        path: Some((
            rw_main_env.base_path().to_path_buf(),
            rw_main_env.labels().clone(),
        )),
        ..*options
    };
    let ro_main_env = rw_main_env.share();

    let mut rw_secondary_envs = vec![];
    let mut ro_secondary_envs = vec![];
    for index in 0..ro_main_env.schema().1.len() {
        let name = secondary_environment_name(index);
        let rw_secondary_env =
            secondary_environment::dump_restore::restore(name, &options, reader).await?;
        let ro_secondary_env = rw_secondary_env.share();

        rw_secondary_envs.push(rw_secondary_env);
        ro_secondary_envs.push(ro_secondary_env);
    }

    indexing_thread_pool
        .lock()
        .add_cache(ro_main_env, rw_secondary_envs);

    Ok(LmdbRwCache {
        main_env: rw_main_env,
        secondary_envs: ro_secondary_envs,
        indexing_thread_pool,
    })
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use dozer_storage::generator::{Generator, IntoGenerator};

    use crate::cache::{
        lmdb::tests::utils::{create_cache, insert_rec_1},
        test_utils, RwCache,
    };

    use super::*;

    #[tokio::test]
    async fn test_dump_restore() {
        let (mut cache, indexing_thread_pool, _, _) = create_cache(test_utils::schema_1);

        insert_rec_1(&mut cache, (0, Some("a".to_string()), None));
        insert_rec_1(&mut cache, (1, None, Some(2)));
        insert_rec_1(&mut cache, (2, Some("b".to_string()), Some(3)));
        cache.commit().unwrap();
        indexing_thread_pool.lock().wait_until_catchup();

        let mut data = vec![];
        {
            let cache = &cache;
            let txn = &begin_dump_txn(cache).unwrap();
            let generator = |context| async move { dump(cache, txn, &context).await.unwrap() };
            let generator = generator.into_generator();
            for item in pin!(generator).into_iter() {
                data.extend_from_slice(&item.unwrap());
            }
        }

        let restored_cache = restore(
            &Default::default(),
            Default::default(),
            indexing_thread_pool.clone(),
            &mut data.as_slice(),
        )
        .await
        .unwrap();

        super::super::main_environment::dump_restore::tests::assert_main_env_equal(
            cache.main_env(),
            restored_cache.main_env(),
        );

        for index in 0..cache.main_env().schema().1.len() {
            super::super::secondary_environment::dump_restore::tests::assert_secondary_env_equal(
                cache.secondary_env(index),
                restored_cache.secondary_env(index),
            );
        }
    }
}
