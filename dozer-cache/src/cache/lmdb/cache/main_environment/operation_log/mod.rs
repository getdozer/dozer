use dozer_storage::{
    errors::StorageError,
    lmdb::{RoCursor, RwTransaction, Transaction},
    KeyIterator, LmdbCounter, LmdbEnvironment, LmdbMap, LmdbSet, RwLmdbEnvironment,
};
use dozer_types::{
    borrow::{Borrow, Cow, IntoOwned},
    serde::{Deserialize, Serialize},
    types::Record,
};

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

#[derive(Debug, Clone, Copy)]
pub struct OperationLog {
    /// Record primary key -> RecordMetadata, empty if schema is append only or has no primary index.
    primary_key_metadata: PrimaryKeyMetadata,
    /// Operation ids of latest `Insert`s. Used to filter out deleted records in query. Empty if schema is append only.
    present_operation_ids: LmdbSet<u64>,
    /// The next operation id. Monotonically increasing.
    next_operation_id: LmdbCounter,
    /// Operation_id -> operation.
    operation_id_to_operation: LmdbMap<u64, Operation>,
}

impl OperationLog {
    pub fn create(env: &mut RwLmdbEnvironment) -> Result<Self, StorageError> {
        let primary_key_metadata = PrimaryKeyMetadata::create(env)?;
        let present_operation_ids = LmdbSet::create(env, Some("present_operation_ids"))?;
        let next_operation_id = LmdbCounter::create(env, Some("next_operation_id"))?;
        let operation_id_to_operation = LmdbMap::create(env, Some("operation_id_to_operation"))?;
        Ok(Self {
            primary_key_metadata,
            present_operation_ids,
            next_operation_id,
            operation_id_to_operation,
        })
    }

    pub fn open<E: LmdbEnvironment>(env: &E) -> Result<Self, StorageError> {
        let primary_key_metadata = PrimaryKeyMetadata::open(env)?;
        let present_operation_ids = LmdbSet::open(env, Some("present_operation_ids"))?;
        let next_operation_id = LmdbCounter::open(env, Some("next_operation_id"))?;
        let operation_id_to_operation = LmdbMap::open(env, Some("operation_id_to_operation"))?;
        Ok(Self {
            primary_key_metadata,
            present_operation_ids,
            next_operation_id,
            operation_id_to_operation,
        })
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
        key: &[u8],
    ) -> Result<Option<RecordMetadata>, StorageError> {
        self.primary_key_metadata.get_present(txn, key)
    }

    pub fn get_deleted_metadata<T: Transaction>(
        &self,
        txn: &T,
        key: &[u8],
    ) -> Result<Option<RecordMetadata>, StorageError> {
        self.primary_key_metadata.get_deleted(txn, key)
    }

    pub fn get_record<T: Transaction>(
        &self,
        txn: &T,
        key: &[u8],
    ) -> Result<Option<CacheRecord>, StorageError> {
        let Some(metadata) = self.get_present_metadata(txn, key)? else {
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
                "Inconsistent state: primary_key_metadata, hash_to_metadata or present_operation_ids contains an insert operation id that is not an Insert operation"
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

    /// Inserts a new record and returns the new record id and version. The record's primary key must not exist.
    pub fn insert_new(
        &self,
        txn: &mut RwTransaction,
        primary_key: Option<&[u8]>,
        record: &Record,
    ) -> Result<RecordMeta, StorageError> {
        if let Some(primary_key) = primary_key {
            debug_assert!(!primary_key.is_empty());
            debug_assert!(self
                .primary_key_metadata
                .get_deleted(txn, primary_key)?
                .is_none());

            // Generate record id from `primary_key_metadata`.
            let record_id = self.primary_key_metadata.count_data(txn)? as u64;
            let record_meta = RecordMeta::new(record_id, INITIAL_RECORD_VERSION);
            self.insert_overwrite(txn, primary_key, record, None, record_meta)?;
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
            Ok(record_meta)
        }
    }

    /// Inserts a record that was deleted before. The given `record_meta` must match what is stored in `primary_key_metadata`.
    /// Meaning: `record_meta == meta`.
    pub fn insert_deleted(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
        record: &Record,
        record_meta: RecordMeta,
    ) -> Result<RecordMeta, StorageError> {
        let check = || {
            let Some(metadata) = self.primary_key_metadata.get_deleted(txn, primary_key)? else {
                return Ok::<_, StorageError>(false);
            };
            let metadata = metadata.borrow();
            Ok(metadata.meta == record_meta && metadata.insert_operation_id.is_none())
        };
        debug_assert!(check()?);

        self.insert_deleted_impl(txn, primary_key, record, record_meta, None)
    }

    /// Inserts a record that was deleted before, without checking invariants.
    fn insert_deleted_impl(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
        record: &Record,
        record_meta: RecordMeta,
        insert_operation_id: Option<u64>,
    ) -> Result<RecordMeta, StorageError> {
        let old = RecordMetadata {
            meta: record_meta,
            insert_operation_id,
        };
        let new_meta = RecordMeta::new(record_meta.id, record_meta.version + 1);
        self.insert_overwrite(txn, primary_key, record, Some(old), new_meta)?;
        Ok(new_meta)
    }

    /// Inserts an record and overwrites its metadata. This function breaks variants of `OperationLog` and should be used with caution.
    fn insert_overwrite(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
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
            self.primary_key_metadata
                .insert_overwrite(txn, primary_key, &old, &new)?;
        } else {
            self.primary_key_metadata.insert(txn, primary_key, &new)?;
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
        Ok(())
    }

    /// Updates an existing record. The given `record_meta` and `insert_operation_id` must match what is stored in `primary_key_metadata`.
    /// Meaning: `record_meta == meta && Some(insert_operation_id) == meta.insert_operation_id`.
    pub fn update(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
        record: &Record,
        record_meta: RecordMeta,
        insert_operation_id: u64,
    ) -> Result<RecordMeta, StorageError> {
        let check = || {
            let Some(metadata) = self.primary_key_metadata.get_present(txn, primary_key)? else {
                return Ok::<_, StorageError>(false);
            };
            let metadata = metadata.borrow();
            Ok(metadata.meta == record_meta
                && metadata.insert_operation_id == Some(insert_operation_id))
        };
        debug_assert!(check()?);

        self.delete_without_updating_metadata(txn, insert_operation_id)?;
        self.insert_deleted_impl(
            txn,
            primary_key,
            record,
            record_meta,
            Some(insert_operation_id),
        )
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
        )
    }

    /// Deletes an existing record. The given `record_meta` and `insert_operation_id` must match what is stored in `primary_key_metadata`.
    /// Meaning: `record_meta == meta && Some(insert_operation_id) == meta.insert_operation_id`.
    pub fn delete(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
        record_meta: RecordMeta,
        insert_operation_id: u64,
    ) -> Result<(), StorageError> {
        let check = || {
            let Some(metadata) = self.primary_key_metadata.get_present(txn, primary_key)? else {
                return Ok::<_, StorageError>(false);
            };
            let metadata = metadata.borrow();
            Ok(metadata.meta == record_meta
                && metadata.insert_operation_id == Some(insert_operation_id))
        };
        debug_assert!(check()?);

        self.delete_without_updating_metadata(txn, insert_operation_id)?;
        self.primary_key_metadata.insert_overwrite(
            txn,
            primary_key,
            &RecordMetadata {
                meta: record_meta,
                insert_operation_id: Some(insert_operation_id),
            },
            &RecordMetadata {
                meta: record_meta,
                insert_operation_id: None,
            },
        )
    }
}

const INITIAL_RECORD_VERSION: u32 = 1_u32;

mod lmdb_val_impl;
mod metadata;
mod primary_key_metadata;

pub use metadata::RecordMetadata;

use self::{metadata::Metadata, primary_key_metadata::PrimaryKeyMetadata};

#[cfg(test)]
mod tests {
    use crate::cache::lmdb::utils::create_env;

    use super::*;

    #[test]
    fn test_operation_log_append_only() {
        let mut env = create_env(&Default::default()).unwrap().0;
        let log = OperationLog::create(&mut env).unwrap();
        let txn = env.txn_mut().unwrap();
        let append_only = true;

        let records = vec![Record::new(None, vec![]); 10];
        for (index, record) in records.iter().enumerate() {
            let record_meta = log.insert_new(txn, None, record).unwrap();
            assert_eq!(record_meta.id, index as u64);
            assert_eq!(record_meta.version, INITIAL_RECORD_VERSION);
            assert_eq!(
                log.count_present_records(txn, append_only).unwrap(),
                index + 1
            );
            assert_eq!(log.next_operation_id(txn).unwrap(), index as u64 + 1);
            assert_eq!(
                log.present_operation_ids(txn, append_only)
                    .unwrap()
                    .map(|result| result.map(IntoOwned::into_owned))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                (0..=index as u64).collect::<Vec<_>>()
            );
            assert!(log
                .contains_operation_id(txn, append_only, index as _)
                .unwrap());
            assert_eq!(
                log.get_record_by_operation_id_unchecked(txn, index as _)
                    .unwrap(),
                CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
            );
            assert_eq!(
                log.get_operation(txn, index as _).unwrap().unwrap(),
                Operation::Insert {
                    record_meta,
                    record: record.clone(),
                }
            );
        }
    }

    #[test]
    fn test_operation_log_with_primary_key() {
        let mut env = create_env(&Default::default()).unwrap().0;
        let log = OperationLog::create(&mut env).unwrap();
        let txn = env.txn_mut().unwrap();
        let append_only = false;

        // Insert a record.
        let record = Record::new(None, vec![]);
        let primary_key = b"primary_key";
        let mut record_meta = log.insert_new(txn, Some(primary_key), &record).unwrap();
        assert_eq!(record_meta.id, 0);
        assert_eq!(record_meta.version, INITIAL_RECORD_VERSION);
        assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
        assert_eq!(
            log.get_record(txn, primary_key).unwrap().unwrap(),
            CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
        );
        assert_eq!(log.next_operation_id(txn).unwrap(), 1);
        assert_eq!(
            log.present_operation_ids(txn, append_only)
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![0]
        );
        assert!(log.contains_operation_id(txn, append_only, 0).unwrap());
        assert_eq!(
            log.get_record_by_operation_id_unchecked(txn, 0).unwrap(),
            CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
        );
        assert_eq!(
            log.get_operation(txn, 0).unwrap().unwrap(),
            Operation::Insert {
                record_meta,
                record: record.clone(),
            }
        );

        // Update the record.
        record_meta = log
            .update(txn, primary_key, &record, record_meta, 0)
            .unwrap();
        assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
        assert_eq!(
            log.get_record(txn, primary_key).unwrap().unwrap(),
            CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
        );
        assert_eq!(log.next_operation_id(txn).unwrap(), 3);
        assert_eq!(
            log.present_operation_ids(txn, append_only)
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![2]
        );
        assert!(log.contains_operation_id(txn, append_only, 2).unwrap());
        assert_eq!(
            log.get_record_by_operation_id_unchecked(txn, 2).unwrap(),
            CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
        );
        assert_eq!(
            log.get_operation(txn, 1).unwrap().unwrap(),
            Operation::Delete { operation_id: 0 }
        );
        assert_eq!(
            log.get_operation(txn, 2).unwrap().unwrap(),
            Operation::Insert {
                record_meta,
                record: record.clone()
            }
        );

        // Delete the record.
        log.delete(txn, primary_key, record_meta, 2).unwrap();
        assert_eq!(log.count_present_records(txn, append_only).unwrap(), 0);
        assert_eq!(log.get_record(txn, primary_key).unwrap(), None);
        assert_eq!(log.next_operation_id(txn).unwrap(), 4);
        assert_eq!(
            log.present_operation_ids(txn, append_only)
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            Vec::<u64>::new(),
        );
        assert!(!log.contains_operation_id(txn, append_only, 2).unwrap());
        assert_eq!(
            log.get_operation(txn, 3).unwrap().unwrap(),
            Operation::Delete { operation_id: 2 }
        );

        // Insert with that primary key again.
        record_meta = log
            .insert_deleted(txn, primary_key, &record, record_meta)
            .unwrap();
        assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
        assert_eq!(
            log.get_record(txn, primary_key).unwrap().unwrap(),
            CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
        );
        assert_eq!(log.next_operation_id(txn).unwrap(), 5);
        assert_eq!(
            log.present_operation_ids(txn, append_only)
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![4]
        );
        assert!(log.contains_operation_id(txn, append_only, 4).unwrap());
        assert_eq!(
            log.get_record_by_operation_id_unchecked(txn, 4).unwrap(),
            CacheRecord::new(record_meta.id, record_meta.version, record.clone()),
        );
        assert_eq!(
            log.get_operation(txn, 4).unwrap().unwrap(),
            Operation::Insert {
                record_meta,
                record: record.clone(),
            }
        );
    }
}
