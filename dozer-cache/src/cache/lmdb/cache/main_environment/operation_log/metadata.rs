use dozer_storage::{
    errors::StorageError,
    lmdb::{RwTransaction, Transaction},
    BorrowEncode, Decode, Encode, Encoded, LmdbKey, LmdbKeyType, LmdbVal,
};
use dozer_types::{
    borrow::{Borrow, Cow, IntoOwned},
    impl_borrow_for_clone_type,
};

use crate::cache::RecordMeta;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecordMetadata {
    /// The record metadata. `id` is consistent across `insert`s and `delete`s. `version` gets updated on every `insert` or `update`.
    pub meta: RecordMeta,
    /// The operation id of the latest `Insert` operation. `None` if the record is deleted.
    pub insert_operation_id: Option<u64>,
}

pub trait Metadata: Copy {
    type Key<'a>;

    /// Returns total number of metadata.
    fn count_data<T: Transaction>(&self, txn: &T) -> Result<usize, StorageError>;

    /// Tries to get metadata using `key`, returning metadata with `insert_operation_id: Some(_)` if it exists.
    fn get_present<T: Transaction>(
        &self,
        txn: &T,
        key: Self::Key<'_>,
    ) -> Result<Option<RecordMetadata>, StorageError>;

    /// Tries to get metadata using `key`, returning metadata with `insert_operation_id: None` if it exists.
    fn get_deleted<T: Transaction>(
        &self,
        txn: &T,
        key: Self::Key<'_>,
    ) -> Result<Option<RecordMetadata>, StorageError>;

    /// Inserts the key value entry `(key, value)`. Caller must ensure (key, value) does not exist.
    fn insert(
        &self,
        txn: &mut RwTransaction,
        key: Self::Key<'_>,
        value: &RecordMetadata,
    ) -> Result<(), StorageError>;

    /// Overrides the key value entry `(key, old)` with `(key, new)`. Caller must ensure (key, old) exists.
    fn insert_overwrite(
        &self,
        txn: &mut RwTransaction,
        key: Self::Key<'_>,
        old: &RecordMetadata,
        new: &RecordMetadata,
    ) -> Result<(), StorageError>;
}

impl_borrow_for_clone_type!(RecordMetadata);

impl BorrowEncode for RecordMetadata {
    type Encode<'a> = &'a RecordMetadata;
}

impl<'a> Encode<'a> for &'a RecordMetadata {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        let mut result = [0; 21];
        if let Some(insert_operation_id) = self.insert_operation_id {
            result[0] = 1;
            result[1..9].copy_from_slice(&insert_operation_id.to_be_bytes());
        } else {
            result[0] = 0;
        }
        result[9..17].copy_from_slice(&self.meta.id.to_be_bytes());
        result[17..21].copy_from_slice(&self.meta.version.to_be_bytes());
        Ok(Encoded::U8x21(result))
    }
}

impl Decode for RecordMetadata {
    fn decode(bytes: &[u8]) -> Result<Cow<Self>, StorageError> {
        let insert_operation_id = if bytes[0] == 1 {
            Some(u64::from_be_bytes(bytes[1..9].try_into().unwrap()))
        } else {
            None
        };
        let id = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let version = u32::from_be_bytes(bytes[17..21].try_into().unwrap());
        Ok(Cow::Owned(RecordMetadata {
            meta: RecordMeta::new(id, version),
            insert_operation_id,
        }))
    }
}

unsafe impl LmdbVal for RecordMetadata {}

unsafe impl LmdbKey for RecordMetadata {
    const TYPE: LmdbKeyType = LmdbKeyType::FixedSizeOtherThanU32OrUsize;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_metadata_encode_decode() {
        let record_metadata = RecordMetadata {
            meta: RecordMeta::new(1, 2),
            insert_operation_id: Some(3),
        };
        let encoded = record_metadata.encode().unwrap();
        let decoded = RecordMetadata::decode(encoded.as_ref())
            .unwrap()
            .into_owned();
        assert_eq!(record_metadata, decoded);

        let record_metadata = RecordMetadata {
            meta: RecordMeta::new(1, 2),
            insert_operation_id: None,
        };
        let encoded = record_metadata.encode().unwrap();
        let decoded = RecordMetadata::decode(encoded.as_ref())
            .unwrap()
            .into_owned();
        assert_eq!(record_metadata, decoded);
    }

    #[test]
    fn test_metadata_order() {
        let metadata1 = RecordMetadata {
            meta: RecordMeta::new(2, 2),
            insert_operation_id: None,
        };
        let metadata2 = RecordMetadata {
            meta: RecordMeta::new(1, 1),
            insert_operation_id: Some(0),
        };
        let encoded1 = metadata1.encode().unwrap();
        let encoded2 = metadata2.encode().unwrap();
        assert!(encoded1 < encoded2);
    }
}
