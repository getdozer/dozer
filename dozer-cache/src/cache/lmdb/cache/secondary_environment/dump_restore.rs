use dozer_storage::{
    errors::StorageError, generator::FutureGeneratorContext, lmdb::Transaction, DumpItem,
    LmdbCounter, LmdbEnvironment, LmdbMultimap, LmdbOption,
};
use dozer_types::{borrow::IntoOwned, log::info};
use tokio::io::AsyncRead;

use crate::{cache::lmdb::utils::create_env, errors::CacheError};

use super::{
    get_cache_options, set_comparator, CacheOptions, RwSecondaryEnvironment, SecondaryEnvironment,
    SecondaryEnvironmentCommon, DATABASE_DB_NAME, INDEX_DEFINITION_DB_NAME,
    NEXT_OPERATION_ID_DB_NAME,
};

pub async fn dump<'txn, E: SecondaryEnvironment, T: Transaction>(
    env: &E,
    txn: &'txn T,
    context: &FutureGeneratorContext<Result<DumpItem<'txn>, StorageError>>,
) -> Result<(), ()> {
    dozer_storage::dump(
        txn,
        INDEX_DEFINITION_DB_NAME,
        env.common().index_definition_option.database(),
        context,
    )
    .await?;
    dozer_storage::dump(
        txn,
        DATABASE_DB_NAME,
        env.common().database.database(),
        context,
    )
    .await?;
    dozer_storage::dump(
        txn,
        NEXT_OPERATION_ID_DB_NAME,
        env.common().next_operation_id.database(),
        context,
    )
    .await
}

pub async fn restore(
    name: String,
    options: CacheOptions,
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<RwSecondaryEnvironment, CacheError> {
    info!("Restoring secondary environment {name} with options {options:?}");
    let mut env = create_env(&get_cache_options(name.clone(), options))?.0;

    info!("Restoring index definition");
    dozer_storage::restore(&mut env, reader).await?;
    info!("Restoring database");
    dozer_storage::restore(&mut env, reader).await?;
    info!("Restoring next operation id");
    dozer_storage::restore(&mut env, reader).await?;

    let index_definition_option = LmdbOption::open(&env, Some(INDEX_DEFINITION_DB_NAME))?;
    let database = LmdbMultimap::open(&env, Some(DATABASE_DB_NAME))?;
    let next_operation_id = LmdbCounter::open(&env, Some(NEXT_OPERATION_ID_DB_NAME))?;

    let index_definition = index_definition_option
        .load(&env.begin_txn()?)?
        .map(IntoOwned::into_owned)
        .ok_or(CacheError::IndexDefinitionNotFound(name))?;

    set_comparator(&env, &index_definition, database)?;

    Ok(RwSecondaryEnvironment {
        env,
        common: SecondaryEnvironmentCommon {
            index_definition,
            index_definition_option,
            database,
            next_operation_id,
        },
    })
}

#[cfg(test)]
pub mod tests {
    use std::pin::pin;

    use super::*;

    use dozer_storage::{
        assert_database_equal,
        generator::{Generator, IntoGenerator},
        LmdbEnvironment,
    };
    use dozer_types::types::{
        Field, FieldDefinition, FieldType, IndexDefinition, Record, Schema, SourceDefinition,
    };

    use crate::cache::lmdb::cache::{MainEnvironment, RwMainEnvironment, RwSecondaryEnvironment};

    pub fn assert_secondary_env_equal<E1: SecondaryEnvironment, E2: SecondaryEnvironment>(
        env1: &E1,
        env2: &E2,
    ) {
        assert_eq!(
            env1.common().index_definition,
            env2.common().index_definition
        );
        let txn1 = env1.begin_txn().unwrap();
        let txn2 = env2.begin_txn().unwrap();
        assert_database_equal(
            &txn1,
            env1.common().index_definition_option.database(),
            &txn2,
            env2.common().index_definition_option.database(),
        );
        assert_database_equal(
            &txn1,
            env1.common().database.database(),
            &txn2,
            env2.common().database.database(),
        );
        assert_database_equal(
            &txn1,
            env1.common().next_operation_id.database(),
            &txn2,
            env2.common().next_operation_id.database(),
        );
    }

    #[tokio::test]
    async fn test_dump_restore() {
        let schema = Schema {
            fields: vec![FieldDefinition {
                name: "test".to_string(),
                typ: FieldType::String,
                nullable: true,
                source: SourceDefinition::Dynamic,
            }],
            primary_index: vec![0],
        };
        let mut main_env = RwMainEnvironment::new(
            Some(&(schema, vec![])),
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        let record_a = Record {
            values: vec![Field::String("a".to_string())],
            lifetime: None,
        };
        let record_b = Record {
            values: vec![Field::String("b".to_string())],
            lifetime: None,
        };
        main_env.insert(&record_a).unwrap();
        main_env.insert(&record_b).unwrap();
        main_env.delete(&record_a).unwrap();
        main_env.commit(&Default::default()).unwrap();

        let mut env = RwSecondaryEnvironment::new(
            &IndexDefinition::SortedInverted(vec![0]),
            "0".to_string(),
            Default::default(),
        )
        .unwrap();
        {
            let log_txn = main_env.begin_txn().unwrap();
            env.index(
                &log_txn,
                main_env.operation_log().clone(),
                "TEMP",
                &Default::default(),
            )
            .unwrap();
            env.commit().unwrap();
        }

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

        let restored_env = restore("0".to_string(), Default::default(), &mut data.as_slice())
            .await
            .unwrap();
        assert_secondary_env_equal(&env, &restored_env);
    }
}
