use dozer_storage::{
    errors::StorageError,
    lmdb::{RoTransaction, Transaction},
    lmdb_storage::{LmdbEnvironmentManager, SharedTransaction},
    BeginTransaction, LmdbCounter, LmdbMultimap, LmdbOption, ReadTransaction,
};
use dozer_types::{borrow::IntoOwned, types::IndexDefinition};

use crate::{
    cache::lmdb::{
        comparator,
        indexer::{build_index, delete_index},
        utils::init_env,
    },
    errors::CacheError,
};

use super::{
    main_environment::{Operation, OperationLog},
    CacheCommonOptions, CacheWriteOptions,
};

pub type SecondaryIndexDatabase = LmdbMultimap<Vec<u8>, u64>;

pub trait SecondaryEnvironment: BeginTransaction {
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
    txn: SharedTransaction,
    database: SecondaryIndexDatabase,
    next_operation_id: LmdbCounter,
}

impl BeginTransaction for RwSecondaryEnvironment {
    type Transaction<'a> = ReadTransaction<'a>;

    fn begin_txn(&self) -> Result<Self::Transaction<'_>, StorageError> {
        self.txn.begin_txn()
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
    pub fn open(
        name: String,
        common_options: &CacheCommonOptions,
        write_options: CacheWriteOptions,
    ) -> Result<Self, CacheError> {
        let (env, database, next_operation_id, index_definition) =
            open_env_with_index_definition(name, common_options, Some(write_options))?;

        Ok(Self {
            index_definition,
            txn: env.create_txn()?,
            database,
            next_operation_id,
        })
    }

    pub fn create(
        index_definition: &IndexDefinition,
        name: String,
        common_options: &CacheCommonOptions,
        write_options: CacheWriteOptions,
    ) -> Result<Self, CacheError> {
        let (env, database, next_operation_id, index_definition_option, old_index_definition) =
            open_env(name.clone(), common_options, Some(write_options))?;

        set_comparator(&env, index_definition, database)?;

        let txn = env.create_txn()?;
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
            let mut txn = txn.write();
            index_definition_option.store(txn.txn_mut(), index_definition)?;
            txn.commit_and_renew()?;
            index_definition.clone()
        };

        Ok(Self {
            index_definition,
            txn,
            database,
            next_operation_id,
        })
    }

    pub fn index<T: Transaction>(
        &self,
        log_txn: &T,
        operation_log: OperationLog,
    ) -> Result<(), CacheError> {
        let main_env_next_operation_id = operation_log.next_operation_id(log_txn)?;

        let mut txn = self.txn.write();
        let txn = txn.txn_mut();
        loop {
            // Start from `next_operation_id`.
            let operation_id = self.next_operation_id.load(txn)?;
            if operation_id >= main_env_next_operation_id {
                return Ok(());
            }
            // Get operation by operation id.
            let operation = operation_log.get_operation(log_txn, operation_id)?;
            match operation {
                Operation::Insert { record, .. } => {
                    // Build secondary index.
                    build_index(
                        txn,
                        self.database,
                        &record,
                        &self.index_definition,
                        operation_id,
                    )?;
                }
                Operation::Delete { operation_id } => {
                    // If the operation is a `Delete`, find the deleted record, which must exist.
                    let Operation::Insert { record, .. } = operation_log.get_operation(log_txn, operation_id)? else {
                        panic!("Insert operation {} not found", operation_id);
                    };
                    // Delete secondary index.
                    delete_index(
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

    pub fn commit(&self) -> Result<(), CacheError> {
        self.txn.write().commit_and_renew().map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct RoSecondaryEnvironment {
    index_definition: IndexDefinition,
    env: LmdbEnvironmentManager,
    database: SecondaryIndexDatabase,
}

impl BeginTransaction for RoSecondaryEnvironment {
    type Transaction<'a> = RoTransaction<'a>;

    fn begin_txn(&self) -> Result<Self::Transaction<'_>, StorageError> {
        self.env.begin_txn()
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
    pub fn new(name: String, common_options: &CacheCommonOptions) -> Result<Self, CacheError> {
        let (env, database, _, index_definition) =
            open_env_with_index_definition(name, common_options, None)?;
        Ok(Self {
            env,
            database,
            index_definition,
        })
    }
}

#[allow(clippy::type_complexity)]
fn open_env(
    name: String,
    common_options: &CacheCommonOptions,
    write_options: Option<CacheWriteOptions>,
) -> Result<
    (
        LmdbEnvironmentManager,
        SecondaryIndexDatabase,
        LmdbCounter,
        LmdbOption<IndexDefinition>,
        Option<IndexDefinition>,
    ),
    CacheError,
> {
    let path = common_options
        .path
        .as_ref()
        .map(|(base_path, main_name)| (base_path.join(format!("{main_name}_index")), name));
    let common_options = CacheCommonOptions {
        path,
        ..*common_options
    };

    let mut env = init_env(&common_options, write_options)?.0;

    let create_if_not_exist = write_options.is_some();
    let database = LmdbMultimap::new(&mut env, Some("database"), create_if_not_exist)?;
    let next_operation_id =
        LmdbCounter::new(&mut env, Some("next_operation_id"), create_if_not_exist)?;
    let index_definition_option =
        LmdbOption::new(&mut env, Some("index_definition"), create_if_not_exist)?;

    let index_definition = index_definition_option
        .load(&env.begin_txn()?)?
        .map(IntoOwned::into_owned);

    Ok((
        env,
        database,
        next_operation_id,
        index_definition_option,
        index_definition,
    ))
}

fn open_env_with_index_definition(
    name: String,
    common_options: &CacheCommonOptions,
    write_options: Option<CacheWriteOptions>,
) -> Result<
    (
        LmdbEnvironmentManager,
        SecondaryIndexDatabase,
        LmdbCounter,
        IndexDefinition,
    ),
    CacheError,
> {
    let (env, database, next_operation_id, _, index_definition) =
        open_env(name.clone(), common_options, write_options)?;
    let index_definition = index_definition.ok_or(CacheError::IndexDefinitionNotFound(name))?;
    set_comparator(&env, &index_definition, database)?;
    Ok((env, database, next_operation_id, index_definition))
}

fn set_comparator(
    env: &LmdbEnvironmentManager,
    index_definition: &IndexDefinition,
    database: SecondaryIndexDatabase,
) -> Result<(), CacheError> {
    if let IndexDefinition::SortedInverted(fields) = index_definition {
        comparator::set_sorted_inverted_comparator(&env.begin_txn()?, database.database(), fields)?;
    }
    Ok(())
}
