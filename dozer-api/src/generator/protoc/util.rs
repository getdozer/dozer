use dozer_types::types::FieldType;

use crate::errors::GenerationError;

pub fn convert_dozer_type_to_proto_type(field_type: FieldType) -> Result<String, GenerationError> {
    match field_type {
        FieldType::Int => Ok("int32".to_owned()),
        FieldType::Float => Ok("float".to_owned()),
        FieldType::Boolean => Ok("bool".to_owned()),
        FieldType::String => Ok("string".to_owned()),
        FieldType::Decimal => Ok("int32".to_owned()),
        FieldType::Timestamp => Ok("google.protobuf.Timestamp".to_owned()),
        FieldType::Bson => Ok("google.protobuf.Any".to_owned()),
        FieldType::Null => Ok("string".to_owned()),
        _ => Err(GenerationError::DozerToProtoTypeNotSupported(format!(
            "{:?}",
            field_type
        ))),
    }
}
