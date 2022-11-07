use indexmap::IndexMap;
use openapiv3::{
    Contact, MediaType, NumberFormat, NumberType, ObjectType, Parameter, ParameterData,
    ParameterSchemaOrContent, PathStyle, ReferenceOr, Response, Schema, SchemaData, SchemaKind,
    StringType, Type, VariantOrUnknownOrEmpty,
};

const CONTACT_NAME: &str = "Dozer Team";
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

pub fn create_path_parameter(
    name: String,
    description: Option<String>,
    required: bool,
    param_type: Type,
) -> Parameter {
    Parameter::Path {
        parameter_data: ParameterData {
            name,
            description,
            required,
            format: ParameterSchemaOrContent::Schema(ReferenceOr::Item(Schema {
                schema_data: SchemaData {
                    ..Default::default()
                },
                schema_kind: SchemaKind::Type(param_type),
            })),
            deprecated: None,
            example: None,
            examples: IndexMap::new(),
            explode: None,
            extensions: IndexMap::new(),
        },
        style: PathStyle::Simple,
    }
}
pub fn create_reference_response(description: String, schema_reference_path: String) -> Response {
    Response {
        description,
        content: indexmap::indexmap! {
            "application/json".to_owned() => MediaType { schema: Some(ReferenceOr::ref_(&schema_reference_path)), ..Default::default() }
        },
        ..Default::default()
    }
}

pub fn convert_cache_to_oapi_schema(
    cache_schema: dozer_types::types::Schema,
    name: String,
) -> Schema {
    let mut properties: IndexMap<String, ReferenceOr<Box<Schema>>> = IndexMap::new();
    let mut required_properties: Vec<String> = Vec::new();
    for field in cache_schema.fields {
        if !field.nullable {
            required_properties.push(field.name.to_owned());
        }
        properties.insert(
            field.name,
            ReferenceOr::boxed_item(Schema {
                schema_data: Default::default(),
                schema_kind: SchemaKind::Type(convert_cache_type_to_schema_type(field.typ)),
            }),
        );
    }

    Schema {
        schema_data: SchemaData {
            description: Some(format!("A representation of {}", name)),
            ..Default::default()
        },
        schema_kind: SchemaKind::Type(Type::Object(ObjectType {
            properties,
            required: required_properties,
            ..Default::default()
        })),
    }
}

fn get_type_by_name(name: &str) -> Type {
    match name {
        "string" => Type::String(StringType {
            ..Default::default()
        }),
        "bool" => Type::Boolean {},
        "float" => Type::Number(NumberType {
            format: VariantOrUnknownOrEmpty::Item(NumberFormat::Float),
            ..Default::default()
        }),
        "decimal" => Type::Number(NumberType {
            format: VariantOrUnknownOrEmpty::Item(NumberFormat::Double),
            ..Default::default()
        }),
        "integer" => Type::Integer(Default::default()),
        _ => Type::String(StringType {
            ..Default::default()
        }),
    }
}
pub fn convert_cache_type_to_schema_type(field_type: dozer_types::types::FieldType) -> Type {
    match field_type {
        dozer_types::types::FieldType::Int => get_type_by_name("string"),
        dozer_types::types::FieldType::Float => get_type_by_name("float"),
        dozer_types::types::FieldType::Boolean => get_type_by_name("bool"),
        dozer_types::types::FieldType::String => get_type_by_name("string"),
        dozer_types::types::FieldType::Binary => get_type_by_name("string"),
        dozer_types::types::FieldType::Decimal => get_type_by_name("string"),
        dozer_types::types::FieldType::Timestamp => get_type_by_name("decimal"),
        dozer_types::types::FieldType::Bson => get_type_by_name("string"),
        dozer_types::types::FieldType::Null => get_type_by_name("string"),
        dozer_types::types::FieldType::UInt => get_type_by_name("string"),
        dozer_types::types::FieldType::Text => get_type_by_name("string"),
        dozer_types::types::FieldType::UIntArray => get_type_by_name("string"),
        dozer_types::types::FieldType::IntArray => get_type_by_name("string"),
        dozer_types::types::FieldType::FloatArray => get_type_by_name("string"),
        dozer_types::types::FieldType::BooleanArray => get_type_by_name("string"),
        dozer_types::types::FieldType::StringArray => get_type_by_name("string"),
    }
}
