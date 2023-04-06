use std::path::{Path, PathBuf};

use dozer_storage::{
    lmdb_storage::{RoLmdbEnvironment, RwLmdbEnvironment},
    LmdbEnvironment, LmdbOption,
};
use dozer_types::models::api_endpoint::{
    ConflictResolution, OnInsertResolutionTypes, OnUpdateResolutionTypes,
};
use dozer_types::{
    borrow::IntoOwned,
    types::{Field, FieldType, Record, Schema, SchemaWithIndex},
};
use tempdir::TempDir;

use crate::{
    cache::{
        index,
        lmdb::utils::{create_env, open_env},
        RecordWithId,
    },
    errors::CacheError,
};

mod operation_log;

pub use operation_log::{Operation, OperationLog};

use super::CacheOptions;

pub trait MainEnvironment: LmdbEnvironment {
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

#[derive(Debug, Clone)]
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
    env: RwLmdbEnvironment,
    common: MainEnvironmentCommon,
    _temp_dir: Option<TempDir>,
    schema: SchemaWithIndex,
    insert_resolution: OnInsertResolutionTypes,
    update_resolution: OnUpdateResolutionTypes,
}

impl LmdbEnvironment for RwMainEnvironment {
    fn env(&self) -> &dozer_storage::lmdb::Environment {
        self.env.env()
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
        let (mut env, (base_path, name), temp_dir) = create_env(options)?;

        let operation_log = OperationLog::create(&mut env)?;
        let schema_option = LmdbOption::create(&mut env, Some("schema"))?;

        let old_schema = schema_option
            .load(&env.begin_txn()?)?
            .map(IntoOwned::into_owned);

        let schema = match (schema, old_schema) {
            (Some(schema), Some(old_schema)) => {
                if &old_schema != schema {
                    return Err(CacheError::SchemaMismatch {
                        name,
                        given: Box::new(schema.clone()),
                        stored: Box::new(old_schema),
                    });
                }
                old_schema
            }
            (Some(schema), None) => {
                schema_option.store(env.txn_mut()?, schema)?;
                env.commit()?;
                schema.clone()
            }
            (None, Some(schema)) => schema,
            (None, None) => return Err(CacheError::SchemaNotFound),
        };

        Ok(Self {
            env,
            common: MainEnvironmentCommon {
                base_path,
                name,
                operation_log,
                intersection_chunk_size: options.intersection_chunk_size,
            },
            schema,
            _temp_dir: temp_dir,
            insert_resolution: OnInsertResolutionTypes::from(conflict_resolution.on_insert),
            update_resolution: OnUpdateResolutionTypes::from(conflict_resolution.on_update),
        })
    }

    pub fn share(&self) -> RoMainEnvironment {
        RoMainEnvironment {
            env: self.env.share(),
            common: self.common.clone(),
            schema: self.schema.clone(),
        }
    }

    /// Inserts the record into the cache and sets the record version. Returns the record id.
    ///
    /// Every time a record with the same primary key is inserted, its version number gets increased by 1.
    pub fn insert(&mut self, record: &mut Record) -> Result<u64, CacheError> {
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

        let txn = self.env.txn_mut()?;
        self.common
            .operation_log
            .insert(txn, record, primary_key.as_deref(), upsert_on_duplicate)?
            .ok_or(CacheError::PrimaryKeyExists)
    }

    /// Deletes the record and returns the record version.
    pub fn delete(&mut self, primary_key: &[u8]) -> Result<u32, CacheError> {
        let txn = self.env.txn_mut()?;
        self.common
            .operation_log
            .delete(txn, primary_key)?
            .ok_or(CacheError::PrimaryKeyNotFound)
    }

    pub fn update(
        &mut self,
        primary_key: &[u8],
        record: &mut Record,
    ) -> Result<(Option<u32>, u64), CacheError> {
        debug_check_schema_record_consistency(&self.schema.0, record);

        let upsert_enabled = self.update_resolution == OnUpdateResolutionTypes::Upsert;
        let pk_vec = index::get_primary_key(&self.schema.0.primary_index, &record.values);

        let txn = self.env.txn_mut()?;

        let deleted_operation_version = self.common.operation_log.delete(txn, primary_key)?;

        match (deleted_operation_version, upsert_enabled) {
            (None, false) => Err(CacheError::PrimaryKeyNotFound),
            (_, _) => {
                let record_id = self
                    .common
                    .operation_log
                    .insert(txn, record, Some(pk_vec.as_slice()), false)?
                    .ok_or(CacheError::PrimaryKeyExists)?;

                Ok((deleted_operation_version, record_id))
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), CacheError> {
        self.env.commit().map_err(Into::into)
    }
}

#[derive(Debug, Clone)]
pub struct RoMainEnvironment {
    env: RoLmdbEnvironment,
    common: MainEnvironmentCommon,
    schema: SchemaWithIndex,
}

impl LmdbEnvironment for RoMainEnvironment {
    fn env(&self) -> &dozer_storage::lmdb::Environment {
        self.env.env()
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
        let (env, (base_path, name), _temp_dir) = open_env(options)?;

        let operation_log = OperationLog::open(&env)?;
        let schema_option = LmdbOption::open(&env, Some("schema"))?;

        let schema = schema_option
            .load(&env.begin_txn()?)?
            .map(IntoOwned::into_owned)
            .ok_or(CacheError::SchemaNotFound)?;

        Ok(Self {
            env,
            common: MainEnvironmentCommon {
                base_path: base_path.to_path_buf(),
                name: name.to_string(),
                operation_log,
                intersection_chunk_size: options.intersection_chunk_size,
            },
            schema,
        })
    }
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
            FieldType::U128 => {
                debug_assert!(value.as_u128().is_some())
            }
            FieldType::Int => {
                debug_assert!(value.as_int().is_some())
            }
            FieldType::I128 => {
                debug_assert!(value.as_i128().is_some())
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
            FieldType::Duration => debug_assert!(value.as_duration().is_some()),
        }
    }
}
