use dozer_storage::{
    errors::StorageError,
    lmdb::{RoCursor, RwTransaction, Transaction},
    KeyIterator, LmdbCounter, LmdbEnvironment, LmdbMap, LmdbSet, RwLmdbEnvironment,
};
use dozer_types::{
    borrow::{Borrow, Cow, IntoOwned},
    labels::Labels,
    log::info,
    serde::{Deserialize, Serialize},
    types::Record,
};
use metrics::{describe_counter, increment_counter};

use crate::cache::{CacheRecord, RecordMeta};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum Operation {
    Delete {
        /// The operation id of an `Insert` operation, which must exist.
        operation_id: u64,
    },
    Insert {
        record_meta: RecordMeta,
        record: Record,
    },
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub enum OperationBorrow<'a> {
    Delete {
        /// The operation id of an `Insert` operation, which must exist.
        operation_id: u64,
    },
    Insert {
        record_meta: RecordMeta,
        record: &'a Record,
    },
}

#[derive(Debug, Clone)]
pub struct OperationLog {
    /// Record primary key -> RecordMetadata, empty if schema is append only or has no primary index.
    primary_key_metadata: PrimaryKeyMetadata,
    /// Record hash -> RecordMetadata, empty if schema is append only or has primary index.
    hash_metadata: HashMetadata,
    /// Operation ids of latest `Insert`s. Used to filter out deleted records in query. Empty if schema is append only.
    present_operation_ids: LmdbSet<u64>,
    /// The next operation id. Monotonically increasing.
    next_operation_id: LmdbCounter,
    /// Operation_id -> operation.
    operation_id_to_operation: LmdbMap<u64, Operation>,
    /// The cache labels.
    labels: Labels,
}

#[derive(Debug, Clone, Copy)]
pub enum MetadataKey<'a> {
    PrimaryKey(&'a [u8]),
    Hash(&'a Record, u64),
}

const PRESENT_OPERATION_IDS_DB_NAME: &str = "present_operation_ids";
const NEXT_OPERATION_ID_DB_NAME: &str = "next_operation_id";
const OPERATION_ID_TO_OPERATION_DB_NAME: &str = "operation_id_to_operation";

const CACHE_OPERATION_COUNTER_NAME: &str = "cache_operation";

impl OperationLog {
    pub fn create(env: &mut RwLmdbEnvironment, labels: Labels) -> Result<Self, StorageError> {
        describe_counter!(
            CACHE_OPERATION_COUNTER_NAME,
            "Number of operations stored in the cache"
        );

        let primary_key_metadata = PrimaryKeyMetadata::create(env)?;
        let hash_metadata = HashMetadata::create(env)?;
        let present_operation_ids = LmdbSet::create(env, Some(PRESENT_OPERATION_IDS_DB_NAME))?;
        let next_operation_id = LmdbCounter::create(env, Some(NEXT_OPERATION_ID_DB_NAME))?;
        let operation_id_to_operation =
            LmdbMap::create(env, Some(OPERATION_ID_TO_OPERATION_DB_NAME))?;
        Ok(Self {
            primary_key_metadata,
            hash_metadata,
            present_operation_ids,
            next_operation_id,
            operation_id_to_operation,
            labels,
        })
    }

    pub fn open<E: LmdbEnvironment>(env: &E, labels: Labels) -> Result<Self, StorageError> {
        let primary_key_metadata = PrimaryKeyMetadata::open(env)?;
        let hash_metadata = HashMetadata::open(env)?;
        let present_operation_ids = LmdbSet::open(env, Some(PRESENT_OPERATION_IDS_DB_NAME))?;
        let next_operation_id = LmdbCounter::open(env, Some(NEXT_OPERATION_ID_DB_NAME))?;
        let operation_id_to_operation =
            LmdbMap::open(env, Some(OPERATION_ID_TO_OPERATION_DB_NAME))?;
        Ok(Self {
            primary_key_metadata,
            hash_metadata,
            present_operation_ids,
            next_operation_id,
            operation_id_to_operation,
            labels,
        })
    }

    pub fn labels(&self) -> &Labels {
        &self.labels
    }

    pub fn count_present_records<T: Transaction>(
        &self,
        txn: &T,
        schema_is_append_only: bool,
    ) -> Result<usize, StorageError> {
        if schema_is_append_only {
            self.operation_id_to_operation.count(txn)
        } else {
            self.present_operation_ids.count(txn)
        }
        .map_err(Into::into)
    }

    pub fn get_present_metadata<T: Transaction>(
        &self,
        txn: &T,
        key: MetadataKey,
    ) -> Result<Option<RecordMetadata>, StorageError> {
        match key {
            MetadataKey::PrimaryKey(key) => self.primary_key_metadata.get_present(txn, key),
            MetadataKey::Hash(record, hash) => self.hash_metadata.get_present(txn, (record, hash)),
        }
    }

    pub fn get_deleted_metadata<T: Transaction>(
        &self,
        txn: &T,
        key: MetadataKey,
    ) -> Result<Option<RecordMetadata>, StorageError> {
        match key {
            MetadataKey::PrimaryKey(key) => self.primary_key_metadata.get_deleted(txn, key),
            MetadataKey::Hash(record, hash) => self.hash_metadata.get_deleted(txn, (record, hash)),
        }
    }

    pub fn get_record<T: Transaction>(
        &self,
        txn: &T,
        key: &[u8],
    ) -> Result<Option<CacheRecord>, StorageError> {
        let Some(metadata) = self.get_present_metadata(txn, MetadataKey::PrimaryKey(key))? else {
            return Ok(None);
        };
        let Some(insert_operation_id) = metadata.insert_operation_id else {
            return Ok(None);
        };
        self.get_record_by_operation_id_unchecked(txn, insert_operation_id)
            .map(Some)
    }

    pub fn next_operation_id<T: Transaction>(&self, txn: &T) -> Result<u64, StorageError> {
        self.next_operation_id.load(txn).map_err(Into::into)
    }

    pub fn present_operation_ids<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
        schema_is_append_only: bool,
    ) -> Result<KeyIterator<'txn, RoCursor<'txn>, u64>, StorageError> {
        // If schema is append only, then all operation ids are latest `Insert`s.
        if schema_is_append_only {
            self.operation_id_to_operation.keys(txn)
        } else {
            self.present_operation_ids.iter(txn)
        }
    }

    pub fn contains_operation_id<T: Transaction>(
        &self,
        txn: &T,
        schema_is_append_only: bool,
        operation_id: u64,
    ) -> Result<bool, StorageError> {
        // If schema is append only, then all operation ids are latest `Insert`s.
        if schema_is_append_only {
            Ok(true)
        } else {
            self.present_operation_ids.contains(txn, &operation_id)
        }
        .map_err(Into::into)
    }

    pub fn get_record_by_operation_id_unchecked<T: Transaction>(
        &self,
        txn: &T,
        operation_id: u64,
    ) -> Result<CacheRecord, StorageError> {
        let Some(Cow::Owned(Operation::Insert {
            record_meta,
            record,
        })) = self.operation_id_to_operation.get(txn, &operation_id)? else {
            panic!(
                "Inconsistent state: primary_key_metadata, hash_metadata or present_operation_ids contains an insert operation id that is not an Insert operation"
            );
        };
        Ok(CacheRecord::new(
            record_meta.id,
            record_meta.version,
            record,
        ))
    }

    pub fn get_operation<T: Transaction>(
        &self,
        txn: &T,
        operation_id: u64,
    ) -> Result<Option<Operation>, StorageError> {
        Ok(self
            .operation_id_to_operation
            .get(txn, &operation_id)?
            .map(IntoOwned::into_owned))
    }

    /// Inserts a new record and returns the new record id and version. If key is primary key, it must not exist.
    pub fn insert_new(
        &self,
        txn: &mut RwTransaction,
        key: Option<MetadataKey>,
        record: &Record,
    ) -> Result<RecordMeta, StorageError> {
        if let Some(key) = key {
            if let MetadataKey::PrimaryKey(key) = key {
                debug_assert!(!key.is_empty());
                debug_assert!(self.primary_key_metadata.get_present(txn, key)?.is_none())
            }

            // Generate record id from metadata.
            let record_id = match key {
                MetadataKey::PrimaryKey(_) => self.primary_key_metadata.count_data(txn)? as u64,
                MetadataKey::Hash(_, _) => self.hash_metadata.count_data(txn)? as u64,
            };
            let record_meta = RecordMeta::new(record_id, INITIAL_RECORD_VERSION);
            self.insert_overwrite(txn, key, record, None, record_meta)?;
            Ok(record_meta)
        } else {
            // Generate operation id. Record id is operation id.
            let operation_id = self.next_operation_id.fetch_add(txn, 1)?;
            let record_meta = RecordMeta::new(operation_id, INITIAL_RECORD_VERSION);
            // Record operation. The operation id must not exist.
            self.operation_id_to_operation.append(
                txn,
                &operation_id,
                OperationBorrow::Insert {
                    record_meta,
                    record,
                },
            )?;
            increment_counter!(CACHE_OPERATION_COUNTER_NAME, self.labels.clone());
            Ok(record_meta)
        }
    }

    /// Inserts a record that was deleted before. The given `record_meta` must match what is stored in metadata.
    /// Meaning there exists `(key, meta)` pair in metadata and `meta == record_meta`.
    pub fn insert_deleted(
        &self,
        txn: &mut RwTransaction,
        key: MetadataKey,
        record: &Record,
        record_meta: RecordMeta,
    ) -> Result<RecordMeta, StorageError> {
        let check = || {
            if let MetadataKey::PrimaryKey(key) = key {
                let Some(metadata) = self.primary_key_metadata.get_deleted(txn, key)? else {
                    return Ok::<_, StorageError>(false);
                };
                let metadata = metadata.borrow();
                Ok(metadata.meta == record_meta && metadata.insert_operation_id.is_none())
            } else {
                Ok(true)
            }
        };
        debug_assert!(check()?);

        self.insert_deleted_impl(txn, key, record, record_meta, None)
    }

    /// Inserts a record that was deleted before, without checking invariants.
    fn insert_deleted_impl(
        &self,
        txn: &mut RwTransaction,
        key: MetadataKey,
        record: &Record,
        record_meta: RecordMeta,
        insert_operation_id: Option<u64>,
    ) -> Result<RecordMeta, StorageError> {
        let old = RecordMetadata {
            meta: record_meta,
            insert_operation_id,
        };
        let new_meta = RecordMeta::new(record_meta.id, record_meta.version + 1);
        self.insert_overwrite(txn, key, record, Some(old), new_meta)?;
        Ok(new_meta)
    }

    /// Inserts an record and overwrites its metadata. This function breaks variants of `OperationLog` and should be used with caution.
    fn insert_overwrite(
        &self,
        txn: &mut RwTransaction,
        key: MetadataKey,
        record: &Record,
        old: Option<RecordMetadata>,
        new_meta: RecordMeta,
    ) -> Result<(), StorageError> {
        // Generation operation id.
        let operation_id = self.next_operation_id.fetch_add(txn, 1)?;

        // Update `primary_key_metadata`.
        let new = RecordMetadata {
            meta: new_meta,
            insert_operation_id: Some(operation_id),
        };
        if let Some(old) = old {
            match key {
                MetadataKey::PrimaryKey(key) => self
                    .primary_key_metadata
                    .insert_overwrite(txn, key, &old, &new)?,
                MetadataKey::Hash(record, hash) => {
                    self.hash_metadata
                        .insert_overwrite(txn, (record, hash), &old, &new)?
                }
            }
        } else {
            match key {
                MetadataKey::PrimaryKey(key) => self.primary_key_metadata.insert(txn, key, &new)?,
                MetadataKey::Hash(record, hash) => {
                    self.hash_metadata.insert(txn, (record, hash), &new)?
                }
            }
        }

        // Update `present_operation_ids`.
        if !self.present_operation_ids.insert(txn, &operation_id)? {
            panic!("Inconsistent state: operation id already exists");
        }

        // Record operation. The operation id must not exist.
        self.operation_id_to_operation.append(
            txn,
            &operation_id,
            OperationBorrow::Insert {
                record_meta: new_meta,
                record,
            },
        )?;
        increment_counter!(CACHE_OPERATION_COUNTER_NAME, self.labels.clone());
        Ok(())
    }

    /// Updates an existing record. The given `record_meta` and `insert_operation_id` must match what is stored in metadata.
    /// Meaning there exists `(key, meta)` pair in metadata and `meta == record_meta` and `meta.insert_operation_id == Some(insert_operation_id)`.
    pub fn update(
        &self,
        txn: &mut RwTransaction,
        key: MetadataKey,
        record: &Record,
        record_meta: RecordMeta,
        insert_operation_id: u64,
    ) -> Result<RecordMeta, StorageError> {
        self.debug_check_record_existence(txn, key, record_meta, insert_operation_id)?;
        self.delete_without_updating_metadata(txn, insert_operation_id)?;
        self.insert_deleted_impl(txn, key, record, record_meta, Some(insert_operation_id))
    }

    // Only checks `primary_key_metadata` because `hash_metadata` will check existence in `insert_overwrite`.
    fn debug_check_record_existence<T: Transaction>(
        &self,
        txn: &T,
        key: MetadataKey,
        record_meta: RecordMeta,
        insert_operation_id: u64,
    ) -> Result<(), StorageError> {
        let check = || {
            if let MetadataKey::PrimaryKey(key) = key {
                let Some(metadata) = self.primary_key_metadata.get_present(txn, key)? else {
                    return Ok::<_, StorageError>(false);
                };
                let metadata = metadata.borrow();
                Ok(metadata.meta == record_meta
                    && metadata.insert_operation_id == Some(insert_operation_id))
            } else {
                Ok(true)
            }
        };
        debug_assert!(check()?);
        Ok(())
    }

    /// Deletes an operation without updating the record metadata. This function breaks variants of `OperationLog` and should be used with caution.
    fn delete_without_updating_metadata(
        &self,
        txn: &mut RwTransaction,
        insert_operation_id: u64,
    ) -> Result<(), StorageError> {
        // The operation id must be present.
        if !self
            .present_operation_ids
            .remove(txn, &insert_operation_id)?
        {
            panic!("Inconsistent state: insert operation id not found")
        }
        // Generate new operation id.
        let operation_id = self.next_operation_id.fetch_add(txn, 1)?;
        // Record delete operation. The operation id must not exist.
        self.operation_id_to_operation.append(
            txn,
            &operation_id,
            OperationBorrow::Delete {
                operation_id: insert_operation_id,
            },
        )?;
        increment_counter!(CACHE_OPERATION_COUNTER_NAME, self.labels.clone());
        Ok(())
    }

    /// Deletes an existing record. The given `record_meta` and `insert_operation_id` must match what is stored in metadata.
    /// Meaning there exists `(key, meta)` pair in metadata and `meta == record_meta` and `meta.insert_operation_id == Some(insert_operation_id)`.
    pub fn delete(
        &self,
        txn: &mut RwTransaction,
        key: MetadataKey,
        record_meta: RecordMeta,
        insert_operation_id: u64,
    ) -> Result<(), StorageError> {
        self.debug_check_record_existence(txn, key, record_meta, insert_operation_id)?;
        self.delete_without_updating_metadata(txn, insert_operation_id)?;

        let old = RecordMetadata {
            meta: record_meta,
            insert_operation_id: Some(insert_operation_id),
        };
        let new = RecordMetadata {
            meta: record_meta,
            insert_operation_id: None,
        };
        match key {
            MetadataKey::PrimaryKey(key) => self
                .primary_key_metadata
                .insert_overwrite(txn, key, &old, &new),
            MetadataKey::Hash(record, hash) => {
                self.hash_metadata
                    .insert_overwrite(txn, (record, hash), &old, &new)
            }
        }
    }

    pub async fn dump<'txn, T: Transaction>(
        &self,
        txn: &'txn T,
        context: &dozer_storage::generator::FutureGeneratorContext<
            Result<dozer_storage::DumpItem<'txn>, StorageError>,
        >,
    ) -> Result<(), ()> {
        dozer_storage::dump(
            txn,
            PrimaryKeyMetadata::DATABASE_NAME,
            self.primary_key_metadata.database(),
            context,
        )
        .await?;
        dozer_storage::dump(
            txn,
            HashMetadata::DATABASE_NAME,
            self.hash_metadata.database(),
            context,
        )
        .await?;
        dozer_storage::dump(
            txn,
            PRESENT_OPERATION_IDS_DB_NAME,
            self.present_operation_ids.database(),
            context,
        )
        .await?;
        dozer_storage::dump(
            txn,
            NEXT_OPERATION_ID_DB_NAME,
            self.next_operation_id.database(),
            context,
        )
        .await?;
        dozer_storage::dump(
            txn,
            OPERATION_ID_TO_OPERATION_DB_NAME,
            self.operation_id_to_operation.database(),
            context,
        )
        .await
    }

    pub async fn restore<'txn, R: tokio::io::AsyncRead + Unpin>(
        env: &mut RwLmdbEnvironment,
        reader: &mut R,
        labels: Labels,
    ) -> Result<Self, dozer_storage::RestoreError> {
        info!("Restoring primary key metadata");
        dozer_storage::restore(env, reader).await?;
        info!("Restoring hash metadata");
        dozer_storage::restore(env, reader).await?;
        info!("Restoring present operation ids");
        dozer_storage::restore(env, reader).await?;
        info!("Restoring next operation id");
        dozer_storage::restore(env, reader).await?;
        info!("Restoring operation id to operation");
        dozer_storage::restore(env, reader).await?;
        Self::open(env, labels).map_err(Into::into)
    }
}

const INITIAL_RECORD_VERSION: u32 = 1_u32;

mod hash_metadata;
mod lmdb_val_impl;
mod metadata;
mod primary_key_metadata;

pub use metadata::RecordMetadata;

use hash_metadata::HashMetadata;
use metadata::Metadata;
use primary_key_metadata::PrimaryKeyMetadata;

#[cfg(test)]
pub mod tests;
