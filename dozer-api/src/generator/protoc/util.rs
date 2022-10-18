use anyhow::bail;
use dozer_types::types::FieldType;

pub fn convert_dozer_type_to_proto_type(field_type: FieldType) -> anyhow::Result<String> {
    let result = match field_type {
        FieldType::Int => "int32".to_owned(),
        FieldType::Float => "float".to_owned(),
        FieldType::Boolean => "bool".to_owned(),
        FieldType::String => "string".to_owned(),
        FieldType::Binary => bail!("Binary not supported"),
        FieldType::Decimal => "int32".to_owned(),
        FieldType::Timestamp => "Timestamp".to_owned(),
        FieldType::Bson => "Any".to_owned(),
        FieldType::Null => "string".to_owned(),
        FieldType::RecordArray(_) => "ListValue".to_owned(),
    };
    Ok(result)
}
