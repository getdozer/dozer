use indexmap::IndexMap;
use openapiv3::{
    NumberFormat, NumberType, ObjectType, ReferenceOr, Schema, SchemaData, SchemaKind, StringType,
    Type, VariantOrUnknownOrEmpty, Contact, Response, MediaType,
};

const CONTACT_NAME: &str = "Dozer-Team";
const CONTACT_WEB_URL: &str = "https://getdozer.io";
const CONTACT_EMAIL: &str = "api@getdozer.io";

pub fn create_contact_info() -> Option<Contact> {
    Some(Contact {
        name: Some(CONTACT_NAME.to_owned()),
        url: Some(CONTACT_WEB_URL.to_owned()),
        email: Some(CONTACT_EMAIL.to_owned()),
        extensions: Default::default(),
    })
}

pub fn create_reference_response(description: String, schema_reference_path: String) -> Response {
    Response {
        description: description,
        content: indexmap::indexmap!{
            "application/json".to_owned() => MediaType { schema: Some(ReferenceOr::Reference { reference: schema_reference_path }), ..Default::default() }
        },
        ..Default::default()
    }
}

pub fn convert_cache_to_oapi_schema(
    cache_schema: dozer_types::types::Schema,
    name: String,
) -> anyhow::Result<Schema> {
    let mut properties: IndexMap<String, ReferenceOr<Box<Schema>>> = IndexMap::new();
    let mut required_properties: Vec<String> = Vec::new();
    for field in cache_schema.fields.to_owned() {
        if !field.nullable {
            required_properties.push(field.name.to_owned());
        }
        properties.insert(
            field.name,
            ReferenceOr::boxed_item(Schema {
                schema_data: Default::default(),
                schema_kind: openapiv3::SchemaKind::Type(convert_cache_type_to_schema_type(
                    field.typ,
                )),
            }),
        );
    }
    
    let result = Schema {
        schema_data: SchemaData {
            description: Some(format!("A representation of {}", name)),
            ..Default::default()
        },
        schema_kind: SchemaKind::Type(Type::Object(ObjectType {
            properties: properties,
            required: required_properties,
            ..Default::default()
        })),
    };
    Ok(result)
}

pub fn convert_cache_type_to_schema_type(field_type: dozer_types::types::FieldType) -> Type {
    let float_type = Type::Number(NumberType {
        format: VariantOrUnknownOrEmpty::Item(NumberFormat::Float),
        multiple_of: None,
        exclusive_minimum: false,
        exclusive_maximum: false,
        minimum: None,
        maximum: None,
        enumeration: [].to_vec(),
    });

    let bool_type = Type::Boolean {};
    let string_type = Type::String(StringType {
        format: VariantOrUnknownOrEmpty::Empty,
        pattern: None,
        enumeration: [].to_vec(),
        min_length: None,
        max_length: None,
    });
    let decimal_type = Type::Number(NumberType {
        format: VariantOrUnknownOrEmpty::Item(NumberFormat::Double),
        multiple_of: None,
        exclusive_minimum: false,
        exclusive_maximum: false,
        minimum: None,
        maximum: None,
        enumeration: [].to_vec(),
    });
    return match field_type {
        dozer_types::types::FieldType::Int => Type::Integer(Default::default()),
        dozer_types::types::FieldType::Float => float_type,
        dozer_types::types::FieldType::Boolean => bool_type,
        dozer_types::types::FieldType::String => string_type,
        dozer_types::types::FieldType::Binary => string_type,
        dozer_types::types::FieldType::Decimal => decimal_type,
        dozer_types::types::FieldType::Timestamp => string_type,
        dozer_types::types::FieldType::Bson => string_type,
        dozer_types::types::FieldType::Null => string_type,
        dozer_types::types::FieldType::RecordArray(_) => string_type,
    };
}
