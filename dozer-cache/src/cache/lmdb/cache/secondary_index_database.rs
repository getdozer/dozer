use dozer_storage::{
    errors::StorageError,
    lmdb::Transaction,
    lmdb_storage::{LmdbEnvironmentManager, LmdbExclusiveTransaction},
    LmdbMultimap,
};
use dozer_types::types::{IndexDefinition, SchemaIdentifier};

use crate::{cache::lmdb::comparator, errors::CacheError};

pub fn new_secondary_index_database_from_env(
    env: &mut LmdbEnvironmentManager,
    schema_id: &SchemaIdentifier,
    index: usize,
    index_definition: &IndexDefinition,
    create_if_not_exist: bool,
) -> Result<LmdbMultimap<[u8], u64>, CacheError> {
    let name = database_name(schema_id, index);

    let result = LmdbMultimap::new_from_env(env, Some(&name), create_if_not_exist)?;

    let txn = env.begin_ro_txn()?;

    if let IndexDefinition::SortedInverted(fields) = index_definition {
        comparator::set_sorted_inverted_comparator(&txn, result.database(), fields)?;
    }

    txn.commit().map_err(StorageError::Lmdb)?;

    Ok(result)
}

pub fn new_secondary_index_database_from_txn(
    txn: &mut LmdbExclusiveTransaction,
    schema_id: &SchemaIdentifier,
    index: usize,
    index_definition: &IndexDefinition,
    create_if_not_exist: bool,
) -> Result<LmdbMultimap<[u8], u64>, CacheError> {
    let name = database_name(schema_id, index);
    let result = LmdbMultimap::new_from_txn(txn, Some(&name), create_if_not_exist)?;

    if let IndexDefinition::SortedInverted(fields) = index_definition {
        comparator::set_sorted_inverted_comparator(txn.txn(), result.database(), fields)?;
    }

    Ok(result)
}

fn database_name(schema_id: &SchemaIdentifier, index: usize) -> String {
    format!("index_#{}_#{}_#{}", schema_id.id, schema_id.version, index)
}
