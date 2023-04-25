use dozer_storage::{
    lmdb::Transaction,
    lmdb_storage::{RoLmdbEnvironment, RwLmdbEnvironment},
    LmdbCounter, LmdbEnvironment, LmdbMultimap, LmdbOption,
};
use dozer_types::{borrow::IntoOwned, log::debug, types::IndexDefinition};

use crate::{
    cache::lmdb::utils::{create_env, open_env},
    errors::CacheError,
};

use super::{
    main_environment::{Operation, OperationLog},
    CacheOptions,
};

mod comparator;
mod indexer;

pub type SecondaryIndexDatabase = LmdbMultimap<Vec<u8>, u64>;

pub trait SecondaryEnvironment: LmdbEnvironment {
    fn index_definition(&self) -> &IndexDefinition;
    fn database(&self) -> SecondaryIndexDatabase;

    fn count_data(&self) -> Result<usize, CacheError> {
        let txn = self.begin_txn()?;
        self.database().count_data(&txn).map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct RwSecondaryEnvironment {
    index_definition: IndexDefinition,
    env: RwLmdbEnvironment,
    database: SecondaryIndexDatabase,
    next_operation_id: LmdbCounter,
}

impl LmdbEnvironment for RwSecondaryEnvironment {
    fn env(&self) -> &dozer_storage::lmdb::Environment {
        self.env.env()
    }
}

impl SecondaryEnvironment for RwSecondaryEnvironment {
    fn index_definition(&self) -> &IndexDefinition {
        &self.index_definition
    }

    fn database(&self) -> SecondaryIndexDatabase {
        self.database
    }
}

impl RwSecondaryEnvironment {
    pub fn new(
        index_definition: &IndexDefinition,
        name: String,
        options: &CacheOptions,
    ) -> Result<Self, CacheError> {
        let mut env = create_env(&get_cache_options(name.clone(), options))?.0;

        let database = LmdbMultimap::create(&mut env, Some("database"))?;
        let next_operation_id = LmdbCounter::create(&mut env, Some("next_operation_id"))?;
        let index_definition_option = LmdbOption::create(&mut env, Some("index_definition"))?;

        let old_index_definition = index_definition_option
            .load(&env.begin_txn()?)?
            .map(IntoOwned::into_owned);

        let index_definition = if let Some(old_index_definition) = old_index_definition {
            if index_definition != &old_index_definition {
                return Err(CacheError::IndexDefinitionMismatch {
                    name,
                    given: index_definition.clone(),
                    stored: old_index_definition,
                });
            }
            old_index_definition
        } else {
            index_definition_option.store(env.txn_mut()?, index_definition)?;
            env.commit()?;
            index_definition.clone()
        };

        set_comparator(&env, &index_definition, database)?;

        Ok(Self {
            index_definition,
            env,
            database,
            next_operation_id,
        })
    }

    pub fn share(&self) -> RoSecondaryEnvironment {
        RoSecondaryEnvironment {
            index_definition: self.index_definition.clone(),
            env: self.env.share(),
            database: self.database,
        }
    }

    /// Returns `true` if the secondary index is up to date.
    pub fn index<T: Transaction>(
        &mut self,
        log_txn: &T,
        operation_log: OperationLog,
    ) -> Result<bool, CacheError> {
        let main_env_next_operation_id = operation_log.next_operation_id(log_txn)?;

        let txn = self.env.txn_mut()?;
        loop {
            // Start from `next_operation_id`.
            let operation_id = self.next_operation_id.load(txn)?;
            if operation_id >= main_env_next_operation_id {
                return Ok(true);
            }
            // Get operation by operation id.
            let Some(operation) = operation_log.get_operation(log_txn, operation_id)? else {
                // We're not able to read this operation yet, try again later.
                debug!("Operation {} not found", operation_id);
                return Ok(false);
            };
            match operation {
                Operation::Insert { record, .. } => {
                    // Build secondary index.
                    indexer::build_index(
                        txn,
                        self.database,
                        &record,
                        &self.index_definition,
                        operation_id,
                    )?;
                }
                Operation::Delete { operation_id } => {
                    // If the operation is a `Delete`, find the deleted record.
                    let Some(operation) = operation_log.get_operation(log_txn, operation_id)? else {
                        // We're not able to read this operation yet, try again later.
                        debug!("Operation {} not found", operation_id);
                        return Ok(false)
                    };
                    let Operation::Insert { record, .. } = operation else {
                        panic!("Insert operation {} not found", operation_id);
                    };
                    // Delete secondary index.
                    indexer::delete_index(
                        txn,
                        self.database,
                        &record,
                        &self.index_definition,
                        operation_id,
                    )?;
                }
            }
            self.next_operation_id.store(txn, operation_id + 1)?;
        }
    }

    pub fn commit(&mut self) -> Result<(), CacheError> {
        self.env.commit().map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct RoSecondaryEnvironment {
    index_definition: IndexDefinition,
    env: RoLmdbEnvironment,
    database: SecondaryIndexDatabase,
}

impl LmdbEnvironment for RoSecondaryEnvironment {
    fn env(&self) -> &dozer_storage::lmdb::Environment {
        self.env.env()
    }
}

impl SecondaryEnvironment for RoSecondaryEnvironment {
    fn index_definition(&self) -> &IndexDefinition {
        &self.index_definition
    }

    fn database(&self) -> SecondaryIndexDatabase {
        self.database
    }
}

impl RoSecondaryEnvironment {
    pub fn new(name: String, options: &CacheOptions) -> Result<Self, CacheError> {
        let env = open_env(&get_cache_options(name.clone(), options))?.0;

        let database = LmdbMultimap::open(&env, Some("database"))?;
        let index_definition_option = LmdbOption::open(&env, Some("index_definition"))?;

        let index_definition = index_definition_option
            .load(&env.begin_txn()?)?
            .map(IntoOwned::into_owned)
            .ok_or(CacheError::IndexDefinitionNotFound(name))?;

        set_comparator(&env, &index_definition, database)?;
        Ok(Self {
            env,
            database,
            index_definition,
        })
    }
}

fn get_cache_options(name: String, options: &CacheOptions) -> CacheOptions {
    let path = options
        .path
        .as_ref()
        .map(|(base_path, main_name)| (base_path.join(format!("{main_name}_index")), name));
    CacheOptions { path, ..*options }
}

fn set_comparator<E: LmdbEnvironment>(
    env: &E,
    index_definition: &IndexDefinition,
    database: SecondaryIndexDatabase,
) -> Result<(), CacheError> {
    if let IndexDefinition::SortedInverted(fields) = index_definition {
        comparator::set_sorted_inverted_comparator(&env.begin_txn()?, database.database(), fields)?;
    }
    Ok(())
}
