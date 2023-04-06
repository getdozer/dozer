use dozer_storage::{errors::StorageError, BorrowEncode, Decode, Encode, Encoded, LmdbVal};
use dozer_types::{
    borrow::{Borrow, Cow, IntoOwned},
    impl_borrow_for_clone_type,
};

use crate::cache::RecordMeta;

use super::{Operation, OperationBorrow, RecordMetadata};

impl_borrow_for_clone_type!(RecordMetadata);

impl BorrowEncode for RecordMetadata {
    type Encode<'a> = &'a RecordMetadata;
}

impl<'a> Encode<'a> for &'a RecordMetadata {
    fn encode(self) -> Result<Encoded<'a>, StorageError> {
        let mut result = [0; 21];
        result[0..8].copy_from_slice(&self.meta.id.to_be_bytes());
        result[8..12].copy_from_slice(&self.meta.version.to_be_bytes());
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
            meta: RecordMeta::new(id, version),
            insert_operation_id,
        }))
    }
}

unsafe impl LmdbVal for RecordMetadata {}

impl<'a> IntoOwned<Operation> for OperationBorrow<'a> {
    fn into_owned(self) -> Operation {
        match self {
            Self::Delete { operation_id } => Operation::Delete { operation_id },
            Self::Insert {
                record_meta,
                record,
            } => Operation::Insert {
                record_meta,
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
            Self::Insert {
                record_meta,
                record,
            } => OperationBorrow::Insert {
                record_meta: *record_meta,
                record,
            },
        }
    }

    fn upcast<'b, 'a: 'b>(borrow: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
        match borrow {
            OperationBorrow::Delete { operation_id } => OperationBorrow::Delete { operation_id },
            OperationBorrow::Insert {
                record_meta,
                record,
            } => OperationBorrow::Insert {
                record_meta,
                record,
            },
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

unsafe impl LmdbVal for Operation {}

#[cfg(test)]
mod tests {
    use dozer_types::types::Record;

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
    fn test_operation_encode_decode() {
        let operation = Operation::Delete { operation_id: 1 };
        let encoded = operation.borrow().encode().unwrap();
        let decoded = Operation::decode(encoded.as_ref()).unwrap().into_owned();
        assert_eq!(operation, decoded);

        let operation = Operation::Insert {
            record_meta: RecordMeta::new(1, 1),
            record: Record::new(None, vec![1.into(), 2.into(), 3.into()], None),
        };
        let encoded = operation.borrow().encode().unwrap();
        let decoded = Operation::decode(encoded.as_ref()).unwrap().into_owned();
        assert_eq!(operation, decoded);
    }
}
