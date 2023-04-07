use dozer_types::{
    indexmap::{self, IndexMap},
    types::{FieldType, DATE_FORMAT},
};
use openapiv3::{
    ArrayType, Contact, IntegerFormat, IntegerType, MediaType, NumberFormat, NumberType,
    ObjectType, Parameter, ParameterData, ParameterSchemaOrContent, PathStyle, ReferenceOr,
    Response, Schema, SchemaData, SchemaKind, StringFormat, StringType, Type,
    VariantOrUnknownOrEmpty,
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

pub fn _create_path_parameter(
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

pub fn create_response(description: String, schema: Schema) -> Response {
    Response {
        description,
        content: indexmap::indexmap! {
            "application/json".to_string() => MediaType { schema: Some(ReferenceOr::Item(schema)), ..Default::default() }
        },
        ..Default::default()
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
    name: &str,
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
            description: Some(format!("A representation of {name}")),
            ..Default::default()
        },
        schema_kind: SchemaKind::Type(Type::Object(ObjectType {
            properties,
            required: required_properties,
            ..Default::default()
        })),
    }
}

/// Should be consistent with `field_to_json_value`.
fn convert_cache_type_to_schema_type(field_type: dozer_types::types::FieldType) -> Type {
    match field_type {
        FieldType::UInt | FieldType::Int => Type::Integer(IntegerType {
            format: VariantOrUnknownOrEmpty::Item(IntegerFormat::Int64),
            ..Default::default()
        }),
        FieldType::U128 | FieldType::I128 => Type::String(StringType {
            format: VariantOrUnknownOrEmpty::Empty,
            ..Default::default()
        }),
        FieldType::Float => Type::Number(NumberType {
            format: VariantOrUnknownOrEmpty::Item(NumberFormat::Double),
            ..Default::default()
        }),
        FieldType::Boolean => Type::Boolean {},
        FieldType::String
        | FieldType::Text
        | FieldType::Decimal
        | FieldType::Timestamp
        | FieldType::Date => {
            let (format, pattern) = if field_type == FieldType::Timestamp {
                (
                    VariantOrUnknownOrEmpty::Item(StringFormat::DateTime),
                    Some("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'".to_string()),
                )
            } else if field_type == FieldType::Date {
                (
                    VariantOrUnknownOrEmpty::Item(StringFormat::Date),
                    Some(DATE_FORMAT.to_string()),
                )
            } else {
                (VariantOrUnknownOrEmpty::Empty, None)
            };
            Type::String(StringType {
                format,
                pattern,
                ..Default::default()
            })
        }
        FieldType::Binary | FieldType::Bson => Type::Array(ArrayType {
            items: Some(ReferenceOr::Item(Box::new(u8_schema()))),
            min_items: None,
            max_items: None,
            unique_items: false,
        }),
        FieldType::Point => {
            let mut properties: IndexMap<String, ReferenceOr<Box<Schema>>> = IndexMap::new();
            let required: Vec<String> = vec!["x".to_string(), "y".to_string()];

            for key in &required {
                properties.insert(
                    key.clone(),
                    ReferenceOr::boxed_item(Schema {
                        schema_data: Default::default(),
                        schema_kind: SchemaKind::Type(convert_cache_type_to_schema_type(
                            FieldType::Float,
                        )),
                    }),
                );
            }

            Type::Object(ObjectType {
                properties,
                required,
                additional_properties: None,
                min_properties: None,
                max_properties: None,
            })
        }
        FieldType::Duration => {
            let mut properties: IndexMap<String, ReferenceOr<Box<Schema>>> = IndexMap::new();
            let required: Vec<String> = vec!["value".to_string(), "time_unit".to_string()];

            for key in &required {
                properties.insert(
                    key.clone(),
                    ReferenceOr::boxed_item(Schema {
                        schema_data: Default::default(),
                        schema_kind: SchemaKind::Type(convert_cache_type_to_schema_type(
                            FieldType::String,
                        )),
                    }),
                );
            }

            Type::Object(ObjectType {
                properties,
                required,
                additional_properties: None,
                min_properties: None,
                max_properties: None,
            })
        },
    }
}

fn u8_schema() -> Schema {
    Schema {
        schema_data: Default::default(),
        schema_kind: SchemaKind::Type(Type::Integer(IntegerType {
            format: VariantOrUnknownOrEmpty::Item(IntegerFormat::Int32),
            ..Default::default()
        })),
    }
}
