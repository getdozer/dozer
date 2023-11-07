use bincode::config;

use crate::types::{field_test_cases, Field};

#[test]
fn test_field_serialize_roundtrip() {
    for field in field_test_cases() {
        let bytes = field.encode();
        let deserialized = Field::decode(&bytes).unwrap();
        assert_eq!(field, deserialized);
    }
}

#[test]
fn test_field_bincode_serialize_roundtrip() {
    for field in field_test_cases() {
        let bytes = bincode::encode_to_vec(&field, config::legacy()).unwrap();
        let (deserialized, _): (Field, _) = bincode::decode_from_slice(&bytes, config::legacy())
            .unwrap_or_else(|e| {
                panic!("Failed to deserialize field: {field:?} from bytes: {bytes:?}. {e}")
            });
        assert_eq!(field, deserialized);
    }
}

#[test]
fn field_serialization_should_never_be_empty() {
    for field in field_test_cases() {
        let bytes = field.encode();
        assert!(!bytes.is_empty());
    }
}

#[test]
fn encoding_len_must_agree_with_encode() {
    for field in field_test_cases() {
        let bytes = field.encode();
        assert_eq!(bytes.len(), field.encoding_len());
    }
}
