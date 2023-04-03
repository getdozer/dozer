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

use crate::cache::RecordWithId;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub enum Operation {
    Delete {
        /// The operation id of an `Insert` operation, which must exist.
        operation_id: u64,
    },
    Insert {
        record_id: u64,
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
        record_id: u64,
        record: &'a Record,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct OperationLog {
    /// Record primary key -> RecordMetadata, empty if schema has no primary key.
    /// Length always increases.
    primary_key_to_metadata: LmdbMap<Vec<u8>, RecordMetadata>,
    /// Operation ids of latest `Insert`s. Used to filter out deleted records in query. Empty if schema has no primary key.
    present_operation_ids: LmdbSet<u64>,
    /// The next operation id. Monotonically increasing.
    next_operation_id: LmdbCounter,
    /// Operation_id -> operation.
    operation_id_to_operation: LmdbMap<u64, Operation>,
}

impl OperationLog {
    pub fn create(env: &mut RwLmdbEnvironment) -> Result<Self, StorageError> {
        let primary_key_to_metadata = LmdbMap::create(env, Some("primary_key_to_metadata"))?;
        let present_operation_ids = LmdbSet::create(env, Some("present_operation_ids"))?;
        let next_operation_id = LmdbCounter::create(env, Some("next_operation_id"))?;
        let operation_id_to_operation = LmdbMap::create(env, Some("operation_id_to_operation"))?;
        Ok(Self {
            primary_key_to_metadata,
            present_operation_ids,
            next_operation_id,
            operation_id_to_operation,
        })
    }

    pub fn open<E: LmdbEnvironment>(env: &E) -> Result<Self, StorageError> {
        let primary_key_to_metadata = LmdbMap::open(env, Some("primary_key_to_metadata"))?;
        let present_operation_ids = LmdbSet::open(env, Some("present_operation_ids"))?;
        let next_operation_id = LmdbCounter::open(env, Some("next_operation_id"))?;
        let operation_id_to_operation = LmdbMap::open(env, Some("operation_id_to_operation"))?;
        Ok(Self {
            primary_key_to_metadata,
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

    pub fn get_record<T: Transaction>(
        &self,
        txn: &T,
        key: &[u8],
    ) -> Result<Option<RecordWithId>, StorageError> {
        let Some(metadata) = self.primary_key_to_metadata.get(txn, key)? else {
            return Ok(None);
        };
        let Some(insert_operation_id) = metadata.borrow().insert_operation_id else {
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
    ) -> Result<RecordWithId, StorageError> {
        let Some(Cow::Owned(Operation::Insert {
            record_id,
            record,
        })) = self.operation_id_to_operation.get(txn, &operation_id)? else {
            panic!(
                "Inconsistent state: primary_key_to_metadata or present_operation_ids contains an insert operation id that is not an Insert operation"
            );
        };
        Ok(RecordWithId::new(record_id, record))
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

    /// Inserts the record and sets the record version. Returns the record id.
    ///
    /// If the record's primary key collides with an existing record, returns `None`.
    ///
    /// Every time a record with the same primary key is inserted, its version number gets increased by 1.
    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        record: &mut Record,
        primary_key: Option<&[u8]>,
        upsert_on_collision: bool,
    ) -> Result<Option<u64>, StorageError> {
        // Calculate operation id and record id.
        let (operation_id, record_id, record_version) = if let Some(primary_key) = primary_key {
            // Get or generate record id from `primary_key_to_metadata`.
            let pk_metadata = self.primary_key_to_metadata.get(txn, primary_key)?;
            let (record_id, record_version, metadata_to_delete) = match pk_metadata {
                // Primary key is never inserted before. Generate new id from `primary_key_to_metadata`.
                None => (
                    self.primary_key_to_metadata.count(txn)? as u64,
                    INITIAL_RECORD_VERSION,
                    None,
                ),
                Some(metadata) => {
                    let metadata = metadata.borrow();
                    if metadata.insert_operation_id.is_none() {
                        // This primary key was deleted. Use the record id from its first insertion.
                        (metadata.id, metadata.version + 1, None)
                    } else if upsert_on_collision {
                        (metadata.id, metadata.version + 1, Some(*metadata))
                    } else {
                        // Primary key collision.
                        return Ok(None);
                    }
                }
            };

            if let Some(metadata) = metadata_to_delete {
                self.execute_deletion(txn, primary_key, metadata)?;
            }

            // Generation operation id.
            let operation_id = self.next_operation_id.fetch_add(txn, 1)?;
            // Update `primary_key_to_metadata` and `present_operation_ids`.
            self.primary_key_to_metadata.insert_overwrite(
                txn,
                primary_key,
                &RecordMetadata {
                    id: record_id,
                    version: record_version,
                    insert_operation_id: Some(operation_id),
                },
            )?;
            if !self.present_operation_ids.insert(txn, &operation_id)? {
                panic!("Inconsistent state: operation id already exists");
            }
            // Update record version.
            (operation_id, record_id, record_version)
        } else {
            // Generation operation id.
            let operation_id = self.next_operation_id.fetch_add(txn, 1)?;
            // If the record has no primary key, record id is operation id.
            (operation_id, operation_id, INITIAL_RECORD_VERSION)
        };

        record.version = Some(record_version);
        // Record operation. The operation id must not exist.
        self.operation_id_to_operation.append(
            txn,
            &operation_id,
            OperationBorrow::Insert { record_id, record },
        )?;
        Ok(Some(record_id))
    }

    /// Deletes the record and returns the record version. Returns `None` if the record does not exist.
    pub fn delete(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
    ) -> Result<Option<u32>, StorageError> {
        // Find operation id by primary key.
        let Some(metadata) = self.primary_key_to_metadata.get(txn, primary_key)? else {
            return Ok(None);
        };

        let metadata = metadata.into_owned();

        self.execute_deletion(txn, primary_key, metadata)
    }

    fn execute_deletion(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
        metadata: RecordMetadata,
    ) -> Result<Option<u32>, StorageError> {
        let Some(insert_operation_id) = metadata.insert_operation_id else {
            return Ok(None);
        };

        // Remove deleted operation id.
        self.primary_key_to_metadata.insert_overwrite(
            txn,
            primary_key,
            &RecordMetadata {
                id: metadata.id,
                version: metadata.version,
                insert_operation_id: None,
            },
        )?;
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
        Ok(Some(metadata.version))
    }
}

const INITIAL_RECORD_VERSION: u32 = 1_u32;

#[derive(Debug, Clone, Copy, PartialEq)]
struct RecordMetadata {
    /// The record id. Consistent across `insert`s and `delete`s.
    id: u64,
    /// The latest record version, even if the record is deleted.
    version: u32,
    /// The operation id of the latest `Insert` operation. `None` if the record is deleted.
    insert_operation_id: Option<u64>,
}

mod lmdb_val_impl;

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

        let mut records = vec![Record::new(None, vec![], None); 10];
        for (index, record) in records.iter_mut().enumerate() {
            let record_id = log.insert(txn, record, None, false).unwrap().unwrap();
            assert_eq!(record_id, index as u64);
            assert_eq!(record.version, Some(INITIAL_RECORD_VERSION));
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
                RecordWithId::new(record_id, record.clone())
            );
            assert_eq!(
                log.get_operation(txn, index as _).unwrap().unwrap(),
                Operation::Insert {
                    record_id,
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
        let mut record = Record::new(None, vec![], None);
        let primary_key = b"primary_key";
        let record_id = log
            .insert(txn, &mut record, Some(primary_key), false)
            .unwrap()
            .unwrap();
        assert_eq!(record_id, 0);
        assert_eq!(record.version, Some(INITIAL_RECORD_VERSION));
        assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
        assert_eq!(
            log.get_record(txn, primary_key).unwrap().unwrap(),
            RecordWithId::new(record_id, record.clone())
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
            RecordWithId::new(record_id, record.clone())
        );
        assert_eq!(
            log.get_operation(txn, 0).unwrap().unwrap(),
            Operation::Insert {
                record_id,
                record: record.clone(),
            }
        );

        // Insert again with the same primary key should fail.
        assert_eq!(
            log.insert(txn, &mut record, Some(primary_key), false)
                .unwrap(),
            None
        );

        // Delete the record.
        let version = log.delete(txn, primary_key).unwrap().unwrap();
        assert_eq!(version, INITIAL_RECORD_VERSION);
        assert_eq!(log.count_present_records(txn, append_only).unwrap(), 0);
        assert_eq!(log.get_record(txn, primary_key).unwrap(), None);
        assert_eq!(log.next_operation_id(txn).unwrap(), 2);
        assert_eq!(
            log.present_operation_ids(txn, append_only)
                .unwrap()
                .map(|result| result.map(IntoOwned::into_owned))
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            Vec::<u64>::new(),
        );
        assert!(!log.contains_operation_id(txn, append_only, 0).unwrap());
        assert_eq!(
            log.get_operation(txn, 1).unwrap().unwrap(),
            Operation::Delete { operation_id: 0 }
        );

        // Delete a non-existing record should fail.
        assert_eq!(log.delete(txn, b"non_existing_primary_key").unwrap(), None);

        // Delete an deleted record should fail.
        assert_eq!(log.delete(txn, primary_key).unwrap(), None);

        // Insert with that primary key again.
        let record_id = log
            .insert(txn, &mut record, Some(primary_key), false)
            .unwrap()
            .unwrap();
        assert_eq!(record_id, 0);
        assert_eq!(record.version, Some(INITIAL_RECORD_VERSION + 1));
        assert_eq!(log.count_present_records(txn, append_only).unwrap(), 1);
        assert_eq!(
            log.get_record(txn, primary_key).unwrap().unwrap(),
            RecordWithId::new(record_id, record.clone())
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
            RecordWithId::new(record_id, record.clone())
        );
        assert_eq!(
            log.get_operation(txn, 2).unwrap().unwrap(),
            Operation::Insert {
                record_id,
                record: record.clone(),
            }
        );
    }
}
