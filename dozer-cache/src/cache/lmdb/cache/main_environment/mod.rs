use std::path::{Path, PathBuf};

use dozer_storage::{
    errors::StorageError,
    lmdb::RoTransaction,
    lmdb_storage::{LmdbEnvironmentManager, SharedTransaction},
    BeginTransaction, LmdbOption, ReadTransaction,
};
use dozer_types::models::api_endpoint::{ConflictResolution, OnInsertResolutionTypes};
use dozer_types::{
    borrow::IntoOwned,
    types::{Field, FieldType, Record, Schema, SchemaWithIndex},
};
use tempdir::TempDir;

use crate::{
    cache::{index, lmdb::utils::init_env, RecordWithId},
    errors::CacheError,
};

mod operation_log;

pub use operation_log::{Operation, OperationLog};

use super::CacheOptions;

pub trait MainEnvironment: BeginTransaction {
    fn common(&self) -> &MainEnvironmentCommon;

    fn schema(&self) -> &SchemaWithIndex;

    fn base_path(&self) -> &Path {
        &self.common().base_path
    }

    fn name(&self) -> &str {
        &self.common().name
    }

    fn operation_log(&self) -> OperationLog {
        self.common().operation_log
    }

    fn intersection_chunk_size(&self) -> usize {
        self.common().intersection_chunk_size
    }

    fn count(&self) -> Result<usize, CacheError> {
        let txn = self.begin_txn()?;
        self.operation_log()
            .count_present_records(&txn, self.schema().0.is_append_only())
            .map_err(Into::into)
    }

    fn get(&self, key: &[u8]) -> Result<RecordWithId, CacheError> {
        let txn = self.begin_txn()?;
        self.operation_log()
            .get_record(&txn, key)?
            .ok_or(CacheError::PrimaryKeyNotFound)
    }
}

#[derive(Debug)]
pub struct MainEnvironmentCommon {
    /// The environment base path.
    base_path: PathBuf,
    /// The environment name.
    name: String,
    /// The operation log.
    operation_log: OperationLog,
    intersection_chunk_size: usize,
}

#[derive(Debug)]
pub struct RwMainEnvironment {
    txn: SharedTransaction,
    common: MainEnvironmentCommon,
    _temp_dir: Option<TempDir>,
    schema: SchemaWithIndex,
    insert_resolution: OnInsertResolutionTypes,
}

impl BeginTransaction for RwMainEnvironment {
    type Transaction<'a> = ReadTransaction<'a>;

    fn begin_txn(&self) -> Result<Self::Transaction<'_>, StorageError> {
        self.txn.begin_txn()
    }
}

impl MainEnvironment for RwMainEnvironment {
    fn common(&self) -> &MainEnvironmentCommon {
        &self.common
    }

    fn schema(&self) -> &SchemaWithIndex {
        &self.schema
    }
}

impl RwMainEnvironment {
    pub fn new(
        schema: Option<&SchemaWithIndex>,
        options: &CacheOptions,
        conflict_resolution: ConflictResolution,
    ) -> Result<Self, CacheError> {
        let (env, common, schema_option, old_schema, temp_dir) = open_env(options, true)?;
        let txn = env.create_txn()?;

        let schema = match (schema, old_schema) {
            (Some(schema), Some(old_schema)) => {
                if &old_schema != schema {
                    return Err(CacheError::SchemaMismatch {
                        name: common.name,
                        given: Box::new(schema.clone()),
                        stored: Box::new(old_schema),
                    });
                }
                old_schema
            }
            (Some(schema), None) => {
                let mut txn = txn.write();
                schema_option.store(txn.txn_mut(), schema)?;
                txn.commit_and_renew()?;
                schema.clone()
            }
            (None, Some(schema)) => schema,
            (None, None) => return Err(CacheError::SchemaNotFound),
        };

        Ok(Self {
            txn,
            common,
            schema,
            _temp_dir: temp_dir,
            insert_resolution: OnInsertResolutionTypes::from(conflict_resolution.on_insert),
        })
    }

    /// Inserts the record into the cache and sets the record version. Returns the record id.
    ///
    /// Every time a record with the same primary key is inserted, its version number gets increased by 1.
    pub fn insert(&self, record: &mut Record) -> Result<u64, CacheError> {
        debug_check_schema_record_consistency(&self.schema.0, record);

        let upsert_on_duplicate = self.insert_resolution == OnInsertResolutionTypes::Update;
        let primary_key = if self.schema.0.is_append_only() && !upsert_on_duplicate {
            None
        } else {
            Some(index::get_primary_key(
                &self.schema.0.primary_index,
                &record.values,
            ))
        };

        let mut txn = self.txn.write();
        let txn = txn.txn_mut();
        self.common
            .operation_log
            .insert(txn, record, primary_key.as_deref(), upsert_on_duplicate)?
            .ok_or(CacheError::PrimaryKeyExists)
    }

    /// Deletes the record and returns the record version.
    pub fn delete(&self, primary_key: &[u8]) -> Result<u32, CacheError> {
        let mut txn = self.txn.write();
        let txn = txn.txn_mut();
        self.common
            .operation_log
            .delete(txn, primary_key)?
            .ok_or(CacheError::PrimaryKeyNotFound)
    }

    pub fn commit(&self) -> Result<(), CacheError> {
        self.txn.write().commit_and_renew().map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct RoMainEnvironment {
    env: LmdbEnvironmentManager,
    common: MainEnvironmentCommon,
    schema: SchemaWithIndex,
}

impl BeginTransaction for RoMainEnvironment {
    type Transaction<'a> = RoTransaction<'a>;

    fn begin_txn(&self) -> Result<Self::Transaction<'_>, StorageError> {
        self.env.begin_txn()
    }
}

impl MainEnvironment for RoMainEnvironment {
    fn common(&self) -> &MainEnvironmentCommon {
        &self.common
    }

    fn schema(&self) -> &SchemaWithIndex {
        &self.schema
    }
}

impl RoMainEnvironment {
    pub fn new(options: &CacheOptions) -> Result<Self, CacheError> {
        let (env, common, _, schema, _) = open_env(options, false)?;
        let schema = schema.ok_or(CacheError::SchemaNotFound)?;
        Ok(Self {
            env,
            common,
            schema,
        })
    }
}

#[allow(clippy::type_complexity)]
fn open_env(
    options: &CacheOptions,
    create_if_not_exist: bool,
) -> Result<
    (
        LmdbEnvironmentManager,
        MainEnvironmentCommon,
        LmdbOption<SchemaWithIndex>,
        Option<SchemaWithIndex>,
        Option<TempDir>,
    ),
    CacheError,
> {
    let (mut env, (base_path, name), temp_dir) = init_env(options, create_if_not_exist)?;

    let operation_log = OperationLog::new(&mut env, create_if_not_exist)?;
    let schema_option = LmdbOption::new(&mut env, Some("schema"), create_if_not_exist)?;

    let schema = schema_option
        .load(&env.begin_txn()?)?
        .map(IntoOwned::into_owned);

    Ok((
        env,
        MainEnvironmentCommon {
            base_path,
            name,
            operation_log,
            intersection_chunk_size: options.intersection_chunk_size,
        },
        schema_option,
        schema,
        temp_dir,
    ))
}

fn debug_check_schema_record_consistency(schema: &Schema, record: &Record) {
    debug_assert_eq!(schema.identifier, record.schema_id);
    debug_assert_eq!(schema.fields.len(), record.values.len());
    for (field, value) in schema.fields.iter().zip(record.values.iter()) {
        if field.nullable && value == &Field::Null {
            continue;
        }
        match field.typ {
            FieldType::UInt => {
                debug_assert!(value.as_uint().is_some())
            }
            FieldType::Int => {
                debug_assert!(value.as_int().is_some())
            }
            FieldType::Float => {
                debug_assert!(value.as_float().is_some())
            }
            FieldType::Boolean => debug_assert!(value.as_boolean().is_some()),
            FieldType::String => debug_assert!(value.as_string().is_some()),
            FieldType::Text => debug_assert!(value.as_text().is_some()),
            FieldType::Binary => debug_assert!(value.as_binary().is_some()),
            FieldType::Decimal => debug_assert!(value.as_decimal().is_some()),
            FieldType::Timestamp => debug_assert!(value.as_timestamp().is_some()),
            FieldType::Date => debug_assert!(value.as_date().is_some()),
            FieldType::Bson => debug_assert!(value.as_bson().is_some()),
            FieldType::Point => debug_assert!(value.as_point().is_some()),
        }
    }
}
