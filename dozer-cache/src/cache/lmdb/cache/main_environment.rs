use dozer_storage::{
    errors::StorageError,
    lmdb::{RwTransaction, Transaction},
    lmdb_storage::LmdbEnvironmentManager,
    BorrowEncode, Decode, Encode, Encoded, LmdbCounter, LmdbMap, LmdbSet, LmdbValue,
};
use dozer_types::{
    borrow::{Borrow, Cow, ToOwned},
    impl_borrow_for_clone_type,
    serde::{Deserialize, Serialize},
    types::{Record, Schema},
};

use crate::{
    cache::{index, RecordWithId},
    errors::CacheError,
};

const INITIAL_RECORD_VERSION: u32 = 1_u32;

#[derive(Debug, Clone, Copy)]
struct RecordMetadata {
    /// The record id. Consistent across `insert`s and `delete`s.
    id: u64,
    /// The latest record version, even if the record is deleted.
    version: u32,
    /// The operation id of the latest `Insert` operation. `None` if the record is deleted.
    insert_operation_id: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(crate = "dozer_types::serde")]
enum Operation {
    Delete {
        /// The operation id of an `Insert` operation, which must exist.
        operation_id: u64,
    },
    Insert {
        record_id: u64,
        record: Record,
    },
}

#[derive(Debug)]
pub struct MainEnvironment {
    /// Record primary key -> RecordMetadata, empty if schema has no primary key.
    /// Length always increases.
    primary_key_to_metadata: LmdbMap<Vec<u8>, RecordMetadata>,
    /// The next operation id. Monotonically increasing.
    next_operation_id: LmdbCounter,
    /// Operation_id -> operation.
    operation_id_to_operation: LmdbMap<u64, Operation>,
    /// Operation ids of latest `Insert`s. Used to filter out deleted records in query.
    present_operation_ids: LmdbSet<u64>,
}

impl MainEnvironment {
    pub fn new(
        env: &mut LmdbEnvironmentManager,
        create_if_not_exist: bool,
    ) -> Result<Self, CacheError> {
        let primary_key_to_metadata =
            LmdbMap::new_from_env(env, Some("primary_key_to_metadata"), create_if_not_exist)?;
        let next_operation_id =
            LmdbCounter::new_from_env(env, Some("next_operation_id"), create_if_not_exist)?;
        let operation_id_to_operation =
            LmdbMap::new_from_env(env, Some("operation_id_to_operation"), create_if_not_exist)?;
        let present_operation_ids =
            LmdbSet::new_from_env(env, Some("present_operation_ids"), create_if_not_exist)?;
        Ok(Self {
            primary_key_to_metadata,
            next_operation_id,
            operation_id_to_operation,
            present_operation_ids,
        })
    }

    pub fn present_operation_ids(&self) -> LmdbSet<u64> {
        self.present_operation_ids
    }

    pub fn get<T: Transaction>(&self, txn: &T, key: &[u8]) -> Result<RecordWithId, CacheError> {
        let metadata = self
            .primary_key_to_metadata
            .get(txn, key)?
            .ok_or(CacheError::PrimaryKeyNotFound)?;
        let Some(insert_operation_id) = metadata.borrow().insert_operation_id else {
            return Err(CacheError::PrimaryKeyNotFound);
        };
        let Some(Cow::Owned(Operation::Insert { record_id, record })) = self.operation_id_to_operation.get(txn, &insert_operation_id)? else {
            panic!("Inconsistent state: metadata insert_operation_id is not an Insert operation");
        };
        Ok(RecordWithId::new(record_id, record))
    }

    pub fn get_by_operation_id<T: Transaction>(
        &self,
        txn: &T,
        operation_id: u64,
    ) -> Result<Option<RecordWithId>, CacheError> {
        if !self.present_operation_ids.contains(txn, &operation_id)? {
            Ok(None)
        } else {
            let Some(Cow::Owned(Operation::Insert { record_id, record })) = self.operation_id_to_operation.get(txn, &operation_id)? else {
                panic!("Inconsistent state: present_operation_ids contains an operation id that is not an Insert operation");
            };
            Ok(Some(RecordWithId::new(record_id, record)))
        }
    }

    /// Inserts the record into the cache and sets the record version. Returns the record id and the operation id.
    ///
    /// Every time a record with the same primary key is inserted, its version number gets increased by 1.
    pub fn insert(
        &self,
        txn: &mut RwTransaction,
        record: &mut Record,
        schema: &Schema,
    ) -> Result<(u64, u64), CacheError> {
        // Generation operation id.
        let operation_id = self.next_operation_id.fetch_add(txn, 1)?;
        // Calculate record id.
        let record_id = if schema.primary_index.is_empty() {
            record.version = Some(INITIAL_RECORD_VERSION);
            // If the record has no primary key, record id is operation id.
            operation_id
        } else {
            let primary_key = index::get_primary_key(&schema.primary_index, &record.values);
            // Get or generate record id from `primary_key_to_metadata`.
            let (record_id, record_version) =
                match self.primary_key_to_metadata.get(txn, &primary_key)? {
                    // Primary key is never inserted before. Generate new id from `primary_key_to_metadata`.
                    None => (
                        self.primary_key_to_metadata.count(txn)? as u64,
                        INITIAL_RECORD_VERSION,
                    ),
                    Some(metadata) => {
                        let metadata = metadata.into_owned();
                        if metadata.insert_operation_id.is_some() {
                            // This primary key is present. It's an error.
                            return Err(CacheError::PrimaryKeyExists);
                        } else {
                            // This primary key was deleted. Use the record id from its first insertion.
                            (metadata.id, metadata.version + 1)
                        }
                    }
                };
            // Update `primary_key_to_metadata`.
            self.primary_key_to_metadata.insert_overwrite(
                txn,
                &primary_key,
                &RecordMetadata {
                    id: record_id,
                    version: record_version,
                    insert_operation_id: Some(operation_id),
                },
            )?;
            record.version = Some(record_version);
            record_id
        };
        // Record operation. The operation id must not exist.
        if !self.present_operation_ids.insert(txn, &operation_id)? {
            panic!("Inconsistent state: operation id already exists");
        }
        if !self.operation_id_to_operation.insert(
            txn,
            &operation_id,
            OperationBorrow::Insert { record_id, record },
        )? {
            panic!("Inconsistent state: operation id already exists");
        }
        Ok((record_id, operation_id))
    }

    /// Deletes the record and returns the record version and the deleted operation id.
    pub fn delete(
        &self,
        txn: &mut RwTransaction,
        primary_key: &[u8],
    ) -> Result<(u32, u64), CacheError> {
        // Find operation id by primary key.
        let Some(Cow::Owned(RecordMetadata {
            id: record_id,
            version: record_version,
            insert_operation_id: Some(insert_operation_id)
        })) = self.primary_key_to_metadata.get(txn, primary_key)? else {
            return Err(CacheError::PrimaryKeyNotFound);
        };
        // Remove deleted operation id.
        self.primary_key_to_metadata.insert_overwrite(
            txn,
            primary_key,
            &RecordMetadata {
                id: record_id,
                version: record_version,
                insert_operation_id: None,
            },
        )?;
        // The operation id be present.
        if !self
            .present_operation_ids
            .remove(txn, &insert_operation_id)?
        {
            panic!("Inconsistent state: insert operation id not found")
        }
        // Generate new operation id.
        let operation_id = self.next_operation_id.fetch_add(txn, 1)?;
        // Record delete operation. The operation id must not exist.
        if !self.operation_id_to_operation.insert(
            txn,
            &operation_id,
            OperationBorrow::Delete {
                operation_id: insert_operation_id,
            },
        )? {
            panic!("Inconsistent state: operation id already exists");
        }
        Ok((record_version, insert_operation_id))
    }
}

impl_borrow_for_clone_type!(RecordMetadata);

impl BorrowEncode for RecordMetadata {
    type Encode<'a> = &'a RecordMetadata;
}

impl<'a> Encode<'a> for &'a RecordMetadata {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        let mut result = [0; 21];
        result[0..8].copy_from_slice(&self.id.to_be_bytes());
        result[8..12].copy_from_slice(&self.version.to_be_bytes());
        if let Some(insert_operation_id) = self.insert_operation_id {
            result[12] = 1;
            result[13..21].copy_from_slice(&insert_operation_id.to_be_bytes());
        } else {
            result[12] = 0;
        }
        Ok(Encoded::U8x21(result))
    }
}

impl Decode for RecordMetadata {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        let id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let version = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        let insert_operation_id = if bytes[12] == 1 {
            Some(u64::from_be_bytes(bytes[13..21].try_into().unwrap()))
        } else {
            None
        };
        Ok(Cow::Owned(RecordMetadata {
            id,
            version,
            insert_operation_id,
        }))
    }
}

unsafe impl LmdbValue for RecordMetadata {}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(crate = "dozer_types::serde")]
enum OperationBorrow<'a> {
    Delete {
        /// The operation id of an `Insert` operation, which must exist.
        operation_id: u64,
    },
    Insert {
        record_id: u64,
        record: &'a Record,
    },
}

impl<'a> ToOwned<Operation> for OperationBorrow<'a> {
    fn to_owned(self) -> Operation {
        match self {
            Self::Delete { operation_id } => Operation::Delete { operation_id },
            Self::Insert { record_id, record } => Operation::Insert {
                record_id,
                record: record.clone(),
            },
        }
    }
}

impl Borrow for Operation {
    type Borrowed<'a> = OperationBorrow<'a>;

    fn borrow(&self) -> Self::Borrowed<'_> {
        match self {
            Self::Delete { operation_id } => OperationBorrow::Delete {
                operation_id: *operation_id,
            },
            Self::Insert { record_id, record } => OperationBorrow::Insert {
                record_id: *record_id,
                record,
            },
        }
    }

    fn upcast<'b, 'a: 'b>(borrow: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
        match borrow {
            OperationBorrow::Delete { operation_id } => OperationBorrow::Delete { operation_id },
            OperationBorrow::Insert { record_id, record } => {
                OperationBorrow::Insert { record_id, record }
            }
        }
    }
}

impl BorrowEncode for Operation {
    type Encode<'a> = OperationBorrow<'a>;
}

impl<'a> Encode<'a> for OperationBorrow<'a> {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        dozer_types::bincode::serialize(&self)
            .map(Encoded::Vec)
            .map_err(|e| StorageError::SerializationError {
                typ: "Operation",
                reason: Box::new(e),
            })
    }
}

impl Decode for Operation {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        dozer_types::bincode::deserialize(bytes)
            .map(Cow::Owned)
            .map_err(|e| StorageError::DeserializationError {
                typ: "Operation",
                reason: Box::new(e),
            })
    }
}

unsafe impl LmdbValue for Operation {}
