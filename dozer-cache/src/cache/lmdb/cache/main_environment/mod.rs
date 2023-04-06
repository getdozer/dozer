use std::path::{Path, PathBuf};

use dozer_storage::{
    errors::StorageError,
    lmdb::{RwTransaction, Transaction},
    lmdb_storage::{RoLmdbEnvironment, RwLmdbEnvironment},
    LmdbEnvironment, LmdbOption,
};
use dozer_types::models::api_endpoint::{
    OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
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
        CacheRecord, RecordMeta, UpsertResult,
    },
    errors::CacheError,
};

mod operation_log;

use operation_log::RecordMetadata;
pub use operation_log::{Operation, OperationLog};

use super::{CacheOptions, CacheWriteOptions};

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

    fn get(&self, key: &[u8]) -> Result<CacheRecord, CacheError> {
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
    write_options: CacheWriteOptions,
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
        write_options: CacheWriteOptions,
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
            write_options,
        })
    }

    pub fn share(&self) -> RoMainEnvironment {
        RoMainEnvironment {
            env: self.env.share(),
            common: self.common.clone(),
            schema: self.schema.clone(),
        }
    }

    pub fn insert(&mut self, record: &Record) -> Result<UpsertResult, CacheError> {
        let txn = self.env.txn_mut()?;
        insert_impl(
            self.common.operation_log,
            txn,
            &self.schema.0,
            record,
            self.write_options.insert_resolution,
        )
    }

    pub fn delete(&mut self, primary_key: &[u8]) -> Result<Option<RecordMeta>, CacheError> {
        let txn = self.env.txn_mut()?;
        let operation_log = self.common.operation_log;

        if let Some((meta, insert_operation_id)) =
            get_existing_record_metadata(operation_log, txn, primary_key)?
        {
            // The record exists.
            operation_log.delete(txn, primary_key, meta, insert_operation_id)?;
            Ok(Some(meta))
        } else {
            // The record does not exist. Resolve the conflict.
            match self.write_options.delete_resolution {
                OnDeleteResolutionTypes::Nothing => Ok(None),
                OnDeleteResolutionTypes::Panic => Err(CacheError::PrimaryKeyNotFound),
            }
        }
    }

    pub fn update(
        &mut self,
        primary_key: &[u8],
        record: &Record,
    ) -> Result<UpsertResult, CacheError> {
        // if old_key == new_key {
        //     match (key_exist, conflict_resolution) {
        //         (true, _) => Updated, // Case 1
        //         (false, Nothing) => Ignored, // Case 2
        //         (false, Upsert) => Inserted, // Case 3
        //         (false, Panic) => Err, // Case 4
        //     }
        // } else {
        //     match (old_key_exist, new_key_exist, conflict_resolution) {
        //         (true, true, Nothing) => Ignored, // Case 5
        //         (true, true, Upsert) => Err, // Case 6
        //         (true, true, Panic) => Err, // Case 7
        //         (true, false, _) => Updated, // Case 8
        //         (false, true, Nothing) => Ignored, // Case 9
        //         (false, true, Upsert) => Err, // Case 10
        //         (false, true, Panic) => Err, // Case 11
        //         (false, false, Nothing) => Ignored, // Case 12
        //         (false, false, Upsert) => Inserted, // Case 13
        //         (false, false, Panic) => Err, // Case 14
        //     }
        // }

        let txn = self.env.txn_mut()?;
        let operation_log = self.common.operation_log;

        if let Some((old_meta, insert_operation_id)) =
            get_existing_record_metadata(operation_log, txn, primary_key)?
        {
            // Case 1, 5, 6, 7, 8.
            let new_primary_key =
                index::get_primary_key(&self.schema.0.primary_index, &record.values);
            if new_primary_key == primary_key {
                // Case 1.
                let new_meta = RecordMeta::new(old_meta.id, old_meta.version + 1);
                operation_log.update(txn, primary_key, record, new_meta, insert_operation_id)?;
                Ok(UpsertResult::Updated { old_meta, new_meta })
            } else {
                // Case 5, 6, 7, 8.
                let new_metadata = operation_log.get_metadata(txn, &new_primary_key)?;
                match new_metadata {
                    Some(RecordMetadata {
                        insert_operation_id: Some(_),
                        ..
                    }) => {
                        // Case 5, 6, 7.
                        if self.write_options.update_resolution == OnUpdateResolutionTypes::Nothing
                        {
                            // Case 5.
                            Ok(UpsertResult::Ignored)
                        } else {
                            // Case 6, 7.
                            Err(CacheError::PrimaryKeyExists)
                        }
                    }
                    Some(RecordMetadata {
                        mut meta,
                        insert_operation_id: None,
                    }) => {
                        // Case 8. Meta from deleted record.
                        meta.version += 1;
                        operation_log.delete(txn, primary_key, old_meta, insert_operation_id)?;
                        operation_log.insert_deleted(txn, &new_primary_key, record, meta)?;
                        Ok(UpsertResult::Updated {
                            old_meta,
                            new_meta: meta,
                        })
                    }
                    None => {
                        // Case 8. Meta from `insert_new`.
                        operation_log.delete(txn, primary_key, old_meta, insert_operation_id)?;
                        let new_meta =
                            operation_log.insert_new(txn, Some(&new_primary_key), record)?;
                        Ok(UpsertResult::Updated { old_meta, new_meta })
                    }
                }
            }
        } else {
            // Case 2, 3, 4, 9, 10, 11, 12, 13.
            match self.write_options.update_resolution {
                OnUpdateResolutionTypes::Nothing => {
                    // Case 2, 9, 12.
                    Ok(UpsertResult::Ignored)
                }
                OnUpdateResolutionTypes::Upsert => {
                    // Case 3, 10, 13.
                    insert_impl(
                        operation_log,
                        txn,
                        &self.schema.0,
                        record,
                        OnInsertResolutionTypes::Panic,
                    )
                }
                OnUpdateResolutionTypes::Panic => {
                    // Case 4, 11, 14.
                    Err(CacheError::PrimaryKeyNotFound)
                }
            }
        }
    }

    pub fn commit(&mut self) -> Result<(), CacheError> {
        self.env.commit().map_err(Into::into)
    }
}

fn insert_impl(
    operation_log: OperationLog,
    txn: &mut RwTransaction,
    schema: &Schema,
    record: &Record,
    insert_resolution: OnInsertResolutionTypes,
) -> Result<UpsertResult, CacheError> {
    debug_check_schema_record_consistency(schema, record);

    if schema.is_append_only() {
        let meta = operation_log.insert_new(txn, None, record)?;
        Ok(UpsertResult::Inserted { meta })
    } else {
        let primary_key = index::get_primary_key(&schema.primary_index, &record.values);
        let metadata = operation_log.get_metadata(txn, &primary_key)?;
        match metadata {
            Some(RecordMetadata {
                meta,
                insert_operation_id: Some(insert_operation_id),
            }) => {
                // The record already exists. Resolve the conflict.
                match insert_resolution {
                    OnInsertResolutionTypes::Nothing => Ok(UpsertResult::Ignored),
                    OnInsertResolutionTypes::Panic => Err(CacheError::PrimaryKeyExists),
                    OnInsertResolutionTypes::Update => {
                        let new_meta = RecordMeta::new(meta.id, meta.version + 1);
                        operation_log.update(
                            txn,
                            &primary_key,
                            record,
                            new_meta,
                            insert_operation_id,
                        )?;
                        Ok(UpsertResult::Updated {
                            old_meta: meta,
                            new_meta,
                        })
                    }
                }
            }
            Some(RecordMetadata {
                mut meta,
                insert_operation_id: None,
            }) => {
                // The record has an id but was deleted.
                meta.version += 1;
                operation_log.insert_deleted(txn, &primary_key, record, meta)?;
                Ok(UpsertResult::Inserted { meta })
            }
            None => {
                // The record does not exist.
                let meta = operation_log.insert_new(txn, Some(&primary_key), record)?;
                Ok(UpsertResult::Inserted { meta })
            }
        }
    }
}

fn get_existing_record_metadata<T: Transaction>(
    operation_log: OperationLog,
    txn: &T,
    primary_key: &[u8],
) -> Result<Option<(RecordMeta, u64)>, StorageError> {
    if let Some(RecordMetadata {
        meta,
        insert_operation_id: Some(insert_operation_id),
    }) = operation_log.get_metadata(txn, primary_key)?
    {
        Ok(Some((meta, insert_operation_id)))
    } else {
        Ok(None)
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

#[cfg(test)]
mod conflict_resolution_tests;
