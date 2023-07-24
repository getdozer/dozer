use dozer_storage::{
    errors::StorageError, generator::FutureGeneratorContext, lmdb::Transaction, DumpItem,
    LmdbEnvironment, LmdbMap, LmdbOption,
};
use dozer_types::{borrow::IntoOwned, log::info};
use tokio::io::AsyncRead;

use crate::{
    cache::{
        lmdb::{cache::CacheOptions, utils::create_env},
        CacheWriteOptions,
    },
    errors::CacheError,
};

use super::{
    MainEnvironment, MainEnvironmentCommon, OperationLog, RwMainEnvironment,
    CONNECTION_SNAPSHOTTING_DONE_DB_NAME, METADATA_DB_NAME, SCHEMA_DB_NAME,
};

pub async fn dump<'txn, E: MainEnvironment, T: Transaction>(
    env: &E,
    txn: &'txn T,
    context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
) -> Result<(), ()> {
    dozer_storage::dump(
        txn,
        SCHEMA_DB_NAME,
        env.common().schema_option.database(),
        context,
    )
    .await?;
    dozer_storage::dump(
        txn,
        METADATA_DB_NAME,
        env.common().metadata.database(),
        context,
    )
    .await?;
    dozer_storage::dump(
        txn,
        CONNECTION_SNAPSHOTTING_DONE_DB_NAME,
        env.common().connection_snapshotting_done.database(),
        context,
    )
    .await?;
    env.common().operation_log.dump(txn, context).await
}

pub async fn restore(
    options: &CacheOptions,
    write_options: CacheWriteOptions,
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<RwMainEnvironment, CacheError> {
    info!("Restoring main environment with options {options:?}");
    let (mut env, (base_path, labels), temp_dir) = create_env(options)?;

    info!("Restoring schema");
    dozer_storage::restore(&mut env, reader).await?;
    info!("Restoring metadata");
    dozer_storage::restore(&mut env, reader).await?;
    info!("Restoring connection snapshotting done");
    dozer_storage::restore(&mut env, reader).await?;
    info!("Restoring operation log");
    let operation_log = OperationLog::restore(&mut env, reader, labels).await?;

    let schema_option = LmdbOption::open(&env, Some(SCHEMA_DB_NAME))?;
    let metadata = LmdbOption::open(&env, Some(METADATA_DB_NAME))?;
    let connection_snapshotting_done =
        LmdbMap::open(&env, Some(CONNECTION_SNAPSHOTTING_DONE_DB_NAME))?;

    let schema = schema_option
        .load(&env.begin_txn()?)?
        .map(IntoOwned::into_owned)
        .ok_or(CacheError::SchemaNotFound)?;

    Ok(RwMainEnvironment {
        env,
        common: MainEnvironmentCommon {
            base_path,
            schema,
            schema_option,
            metadata,
            connection_snapshotting_done,
            operation_log,
            intersection_chunk_size: options.intersection_chunk_size,
        },
        _temp_dir: temp_dir,
        write_options,
    })
}

#[cfg(test)]
pub mod tests {
    use std::pin::pin;

    use super::*;

    use dozer_storage::{
        assert_database_equal,
        generator::{Generator, IntoGenerator},
    };
    use dozer_types::types::{Record, Schema};

    use crate::cache::lmdb::cache::{
        main_environment::operation_log::tests::assert_operation_log_equal, RwMainEnvironment,
    };

    pub fn assert_main_env_equal<E1: MainEnvironment, E2: MainEnvironment>(env1: &E1, env2: &E2) {
        assert_eq!(env1.common().schema, env2.common().schema);
        let txn1 = env1.begin_txn().unwrap();
        let txn2 = env2.begin_txn().unwrap();
        assert_database_equal(
            &txn1,
            env1.common().schema_option.database(),
            &txn2,
            env2.common().schema_option.database(),
        );
        assert_database_equal(
            &txn1,
            env1.common().metadata.database(),
            &txn2,
            env2.common().metadata.database(),
        );
        assert_database_equal(
            &txn1,
            env1.common().connection_snapshotting_done.database(),
            &txn2,
            env2.common().connection_snapshotting_done.database(),
        );
        assert_operation_log_equal(
            &env1.common().operation_log,
            &txn1,
            &env2.common().operation_log,
            &txn2,
        );
    }

    #[tokio::test]
    async fn test_dump_restore() {
        let schema = Schema::default();
        let mut env = RwMainEnvironment::new(
            Some(&(schema, vec![])),
            None,
            &Default::default(),
            Default::default(),
        )
        .unwrap();

        let record = Record::new(vec![]);
        env.insert(&record).unwrap();
        env.insert(&record).unwrap();
        env.delete(&record).unwrap();
        env.commit().unwrap();

        let mut data = vec![];
        {
            let env = &env;
            let txn = &env.begin_txn().unwrap();
            let generator = |context| async move { dump(env, txn, &context).await.unwrap() };
            let generator = generator.into_generator();
            for item in pin!(generator).into_iter() {
                data.extend_from_slice(&item.unwrap());
            }
        }

        let restored_env = restore(
            &Default::default(),
            Default::default(),
            &mut data.as_slice(),
        )
        .await
        .unwrap();

        assert_main_env_equal(&env, &restored_env);
    }
}
