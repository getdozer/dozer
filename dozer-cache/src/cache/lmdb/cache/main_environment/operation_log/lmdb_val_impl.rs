use dozer_storage::{errors::StorageError, BorrowEncode, Decode, Encode, Encoded, LmdbVal};
use dozer_types::borrow::{Borrow, Cow, IntoOwned};

use super::{Operation, OperationBorrow};

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

    use crate::cache::RecordMeta;

    use super::*;

    #[test]
    fn test_operation_encode_decode() {
        let operation = Operation::Delete { operation_id: 1 };
        let encoded = operation.borrow().encode().unwrap();
        let decoded = Operation::decode(encoded.as_ref()).unwrap().into_owned();
        assert_eq!(operation, decoded);

        let operation = Operation::Insert {
            record_meta: RecordMeta::new(1, 1),
            record: Record::new(None, vec![1.into(), 2.into(), 3.into()]),
        };
        let encoded = operation.borrow().encode().unwrap();
        let decoded = Operation::decode(encoded.as_ref()).unwrap().into_owned();
        assert_eq!(operation, decoded);
    }
}
