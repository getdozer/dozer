use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use dozer_storage::{
    errors::StorageError,
    lmdb::{RwTransaction, Transaction},
    lmdb_storage::{RoLmdbEnvironment, RwLmdbEnvironment},
    LmdbEnvironment, LmdbMap, LmdbOption,
};
use dozer_types::{
    borrow::IntoOwned,
    labels::Labels,
    types::{Field, FieldType, Record, Schema, SchemaWithIndex},
};
use dozer_types::{
    log::warn,
    models::api_endpoint::{
        OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
    },
};
use tempdir::TempDir;

use crate::{
    cache::{
        index,
        lmdb::utils::{create_env, open_env},
        CacheRecord, RecordMeta, UpsertResult,
    },
    errors::{CacheError, ConnectionMismatch},
};

mod operation_log;

use operation_log::RecordMetadata;
pub use operation_log::{Operation, OperationLog};

use self::operation_log::MetadataKey;

use super::{CacheOptions, CacheWriteOptions};

pub trait MainEnvironment: LmdbEnvironment {
    fn common(&self) -> &MainEnvironmentCommon;

    fn schema(&self) -> &SchemaWithIndex {
        &self.common().schema
    }

    fn base_path(&self) -> &Path {
        &self.common().base_path
    }

    fn labels(&self) -> &Labels {
        &self.common().labels
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

    fn metadata(&self) -> Result<Option<u64>, CacheError> {
        let txn = self.begin_txn()?;
        self.common()
            .metadata
            .load(&txn)
            .map(|data| data.map(IntoOwned::into_owned))
            .map_err(Into::into)
    }

    fn is_snapshotting_done(&self) -> Result<bool, CacheError> {
        let txn = self.begin_txn()?;
        for value in self.common().connection_snapshotting_done.values(&txn)? {
            if !value?.into_owned() {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct MainEnvironmentCommon {
    /// The environment base path.
    base_path: PathBuf,
    /// The environment labels.
    labels: Labels,
    /// The schema.
    schema: SchemaWithIndex,
    /// The metadata.
    metadata: LmdbOption<u64>,
    /// The source status.
    connection_snapshotting_done: LmdbMap<String, bool>,
    /// The operation log.
    operation_log: OperationLog,
    intersection_chunk_size: usize,
}

#[derive(Debug)]
pub struct RwMainEnvironment {
    env: RwLmdbEnvironment,
    common: MainEnvironmentCommon,
    _temp_dir: Option<TempDir>,
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
}

impl RwMainEnvironment {
    pub fn new(
        schema: Option<&SchemaWithIndex>,
        connections: Option<&HashSet<String>>,
        options: &CacheOptions,
        write_options: CacheWriteOptions,
    ) -> Result<Self, CacheError> {
        let (mut env, (base_path, labels), temp_dir) = create_env(options)?;

        let operation_log = OperationLog::create(&mut env)?;
        let schema_option = LmdbOption::create(&mut env, Some("schema"))?;
        let metadata = LmdbOption::create(&mut env, Some("metadata"))?;
        let connection_snapshotting_done =
            LmdbMap::create(&mut env, Some("connection_snapshotting_done"))?;

        let old_schema = schema_option
            .load(&env.begin_txn()?)?
            .map(IntoOwned::into_owned);

        let schema = match (schema, old_schema) {
            (Some(schema), Some(old_schema)) => {
                if &old_schema != schema {
                    return Err(CacheError::SchemaMismatch {
                        name: labels.to_string(),
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

        if let Some(connections) = connections {
            if connection_snapshotting_done.count(&env.begin_txn()?)? == 0 {
                // A new environment, set all connections to false.
                let txn = env.txn_mut()?;
                for connection in connections {
                    connection_snapshotting_done.insert(txn, connection.as_str(), &false)?;
                }
                env.commit()?;
            } else {
                // Check if the connections match.
                let mut existing_connections = HashSet::<String>::default();
                for connection in connection_snapshotting_done.iter(&env.begin_txn()?)? {
                    existing_connections.insert(connection?.0.into_owned());
                }
                if &existing_connections != connections {
                    return Err(CacheError::ConnectionsMismatch(Box::new(
                        ConnectionMismatch {
                            name: labels.to_string(),
                            given: connections.clone(),
                            stored: existing_connections,
                        },
                    )));
                }
            }
        }

        Ok(Self {
            env,
            common: MainEnvironmentCommon {
                base_path,
                labels,
                schema,
                metadata,
                connection_snapshotting_done,
                operation_log,
                intersection_chunk_size: options.intersection_chunk_size,
            },
            _temp_dir: temp_dir,
            write_options,
        })
    }

    pub fn share(&self) -> RoMainEnvironment {
        RoMainEnvironment {
            env: self.env.share(),
            common: self.common.clone(),
        }
    }

    pub fn insert(&mut self, record: &Record) -> Result<UpsertResult, CacheError> {
        let txn = self.env.txn_mut()?;
        insert_impl(
            self.common.operation_log,
            txn,
            &self.common.schema.0,
            record,
            self.write_options.insert_resolution,
        )
    }

    pub fn delete(&mut self, record: &Record) -> Result<Option<RecordMeta>, CacheError> {
        if self.common.schema.0.is_append_only() {
            return Err(CacheError::AppendOnlySchema);
        }

        let txn = self.env.txn_mut()?;
        let operation_log = self.common.operation_log;
        let key = calculate_key(&self.common.schema.0, record);

        if let Some((meta, insert_operation_id)) =
            get_existing_record_metadata(operation_log, txn, &key)?
        {
            // The record exists.
            operation_log.delete(txn, key.as_ref(), meta, insert_operation_id)?;
            Ok(Some(meta))
        } else {
            // The record does not exist. Resolve the conflict.
            match self.write_options.delete_resolution {
                OnDeleteResolutionTypes::Nothing => {
                    warn!("Record (Key: {:?}) not found, ignoring delete", key);
                    Ok(None)
                }
                OnDeleteResolutionTypes::Panic => Err(CacheError::PrimaryKeyNotFound),
            }
        }
    }

    pub fn update(&mut self, old: &Record, new: &Record) -> Result<UpsertResult, CacheError> {
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
        let old_key = calculate_key(&self.common.schema.0, old);

        if let Some((old_meta, insert_operation_id)) =
            get_existing_record_metadata(operation_log, txn, &old_key)?
        {
            // Case 1, 5, 6, 7, 8.
            let new_key = calculate_key(&self.common.schema.0, new);
            if new_key.equal(&old_key) {
                // Case 1.
                let new_meta = operation_log.update(
                    txn,
                    old_key.as_ref(),
                    new,
                    old_meta,
                    insert_operation_id,
                )?;
                Ok(UpsertResult::Updated { old_meta, new_meta })
            } else {
                // Case 5, 6, 7, 8.
                let new_metadata = operation_log.get_deleted_metadata(txn, new_key.as_ref())?;
                match new_metadata {
                    Some(RecordMetadata {
                        insert_operation_id: Some(_),
                        ..
                    }) => {
                        // Case 5, 6, 7.
                        if self.write_options.update_resolution == OnUpdateResolutionTypes::Nothing
                        {
                            // Case 5.
                            warn!("Old record (Key: {:?}) and new record (Key: {:?}) both exist, ignoring update", old_key, new_key);
                            Ok(UpsertResult::Ignored)
                        } else {
                            // Case 6, 7.
                            Err(CacheError::PrimaryKeyExists)
                        }
                    }
                    Some(RecordMetadata {
                        meta,
                        insert_operation_id: None,
                    }) => {
                        // Case 8. Meta from deleted record.
                        operation_log.delete(
                            txn,
                            old_key.as_ref(),
                            old_meta,
                            insert_operation_id,
                        )?;
                        let new_meta =
                            operation_log.insert_deleted(txn, new_key.as_ref(), new, meta)?;
                        Ok(UpsertResult::Updated { old_meta, new_meta })
                    }
                    None => {
                        // Case 8. Meta from `insert_new`.
                        operation_log.delete(
                            txn,
                            old_key.as_ref(),
                            old_meta,
                            insert_operation_id,
                        )?;
                        let new_meta =
                            operation_log.insert_new(txn, Some(new_key.as_ref()), new)?;
                        Ok(UpsertResult::Updated { old_meta, new_meta })
                    }
                }
            }
        } else {
            // Case 2, 3, 4, 9, 10, 11, 12, 13.
            match self.write_options.update_resolution {
                OnUpdateResolutionTypes::Nothing => {
                    // Case 2, 9, 12.
                    warn!("Old record (Key: {:?}) not found, ignoring update", old_key);
                    Ok(UpsertResult::Ignored)
                }
                OnUpdateResolutionTypes::Upsert => {
                    // Case 3, 10, 13.
                    insert_impl(
                        operation_log,
                        txn,
                        &self.common.schema.0,
                        new,
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

    pub fn set_metadata(&mut self, metadata: u64) -> Result<(), CacheError> {
        let txn = self.env.txn_mut()?;
        self.common
            .metadata
            .store(txn, &metadata)
            .map_err(Into::into)
    }

    pub fn set_connection_snapshotting_done(
        &mut self,
        connection_name: &str,
    ) -> Result<(), CacheError> {
        let txn = self.env.txn_mut()?;
        self.common
            .connection_snapshotting_done
            .insert_overwrite(txn, connection_name, &true)
            .map_err(Into::into)
    }

    pub fn commit(&mut self) -> Result<(), CacheError> {
        self.env.commit().map_err(Into::into)
    }
}

#[derive(Debug)]
enum OwnedMetadataKey<'a> {
    PrimaryKey(Vec<u8>),
    Hash(&'a Record, u64),
}

impl<'a> OwnedMetadataKey<'a> {
    fn as_ref(&self) -> MetadataKey<'_> {
        match self {
            OwnedMetadataKey::PrimaryKey(key) => MetadataKey::PrimaryKey(key),
            OwnedMetadataKey::Hash(record, hash) => MetadataKey::Hash(record, *hash),
        }
    }

    fn equal(&self, other: &OwnedMetadataKey) -> bool {
        match (self, other) {
            (OwnedMetadataKey::PrimaryKey(key1), OwnedMetadataKey::PrimaryKey(key2)) => {
                key1 == key2
            }
            (OwnedMetadataKey::Hash(_, hash1), OwnedMetadataKey::Hash(_, hash2)) => hash1 == hash2,
            _ => false,
        }
    }
}

fn calculate_key<'a>(schema: &Schema, record: &'a Record) -> OwnedMetadataKey<'a> {
    if schema.primary_index.is_empty() {
        let mut hasher = ahash::AHasher::default();
        record.hash(&mut hasher);
        let hash = hasher.finish();
        OwnedMetadataKey::Hash(record, hash)
    } else {
        let key = index::get_primary_key(&schema.primary_index, &record.values);
        OwnedMetadataKey::PrimaryKey(key)
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
        let key = calculate_key(schema, record);
        let metadata = operation_log.get_deleted_metadata(txn, key.as_ref())?;
        match metadata {
            Some(RecordMetadata {
                meta,
                insert_operation_id: Some(insert_operation_id),
            }) => {
                // The record already exists.
                if schema.primary_index.is_empty() {
                    // Insert anyway.
                    let meta = operation_log.insert_new(txn, Some(key.as_ref()), record)?;
                    Ok(UpsertResult::Inserted { meta })
                } else {
                    // Resolve the conflict.
                    match insert_resolution {
                        OnInsertResolutionTypes::Nothing => {
                            warn!("Record (Key: {:?}) already exist, ignoring insert", key);
                            Ok(UpsertResult::Ignored)
                        }
                        OnInsertResolutionTypes::Panic => Err(CacheError::PrimaryKeyExists),
                        OnInsertResolutionTypes::Update => {
                            let new_meta = operation_log.update(
                                txn,
                                key.as_ref(),
                                record,
                                meta,
                                insert_operation_id,
                            )?;
                            Ok(UpsertResult::Updated {
                                old_meta: meta,
                                new_meta,
                            })
                        }
                    }
                }
            }
            Some(RecordMetadata {
                meta,
                insert_operation_id: None,
            }) => {
                // The record has an id but was deleted.
                let new_meta = operation_log.insert_deleted(txn, key.as_ref(), record, meta)?;
                Ok(UpsertResult::Inserted { meta: new_meta })
            }
            None => {
                // The record does not exist.
                let meta = operation_log.insert_new(txn, Some(key.as_ref()), record)?;
                Ok(UpsertResult::Inserted { meta })
            }
        }
    }
}

fn get_existing_record_metadata<T: Transaction>(
    operation_log: OperationLog,
    txn: &T,
    key: &OwnedMetadataKey,
) -> Result<Option<(RecordMeta, u64)>, StorageError> {
    if let Some(RecordMetadata {
        meta,
        insert_operation_id: Some(insert_operation_id),
    }) = operation_log.get_present_metadata(txn, key.as_ref())?
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
}

impl RoMainEnvironment {
    pub fn new(options: &CacheOptions) -> Result<Self, CacheError> {
        let (env, (base_path, labels), _temp_dir) = open_env(options)?;

        let operation_log = OperationLog::open(&env)?;
        let schema_option = LmdbOption::open(&env, Some("schema"))?;
        let metadata = LmdbOption::open(&env, Some("metadata"))?;
        let connection_snapshotting_done =
            LmdbMap::open(&env, Some("connection_snapshotting_done"))?;

        let schema = schema_option
            .load(&env.begin_txn()?)?
            .map(IntoOwned::into_owned)
            .ok_or(CacheError::SchemaNotFound)?;

        Ok(Self {
            env,
            common: MainEnvironmentCommon {
                base_path: base_path.to_path_buf(),
                labels: labels.clone(),
                schema,
                metadata,
                connection_snapshotting_done,
                operation_log,
                intersection_chunk_size: options.intersection_chunk_size,
            },
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
            FieldType::Json => debug_assert!(value.as_json().is_some()),
            FieldType::Point => debug_assert!(value.as_point().is_some()),
            FieldType::Duration => debug_assert!(value.as_duration().is_some()),
        }
    }
}

#[cfg(test)]
mod conflict_resolution_tests;

#[cfg(test)]
mod hash_tests;
