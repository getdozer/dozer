use super::utils::{convert_cache_to_oapi_schema, create_contact_info, create_reference_response};
use crate::errors::GenerationError;
use dozer_types::indexmap::{self, IndexMap};
use dozer_types::serde_json;
use dozer_types::types::IndexDefinition;
use dozer_types::{models::api_endpoint::ApiEndpoint, types::FieldType};
use openapiv3::*;
use serde_json::{json, Value};
use tempdir::TempDir;

pub struct OpenApiGenerator {
    schema: dozer_types::types::Schema,
    secondary_indexes: Vec<IndexDefinition>,
    schema_name: String,
    endpoint: ApiEndpoint,
    server_host: Vec<String>,
}
impl OpenApiGenerator {
    fn get_singular_name(&self) -> String {
        self.schema_name.to_owned()
    }
    fn get_plural_name(&self) -> String {
        format!("{}_array", self.schema_name.to_owned())
    }

    // Generate first secondary_index as an example
    fn generate_query_example(&self) -> Value {
        if !self.secondary_indexes.is_empty() {
            if let IndexDefinition::SortedInverted(fields) = &self.secondary_indexes[0] {
                let field_def = &self.schema.fields[fields[0].0];
                let name = field_def.name.clone();
                let val = match field_def.typ {
                    FieldType::Int => Value::from(-1),
                    FieldType::UInt => Value::from(-1),
                    FieldType::Float => Value::from(1.1),
                    FieldType::Boolean => Value::from(true),
                    FieldType::String => Value::from("foo".to_string()),
                    FieldType::Binary
                    | FieldType::Decimal
                    | FieldType::Timestamp
                    | FieldType::Bson
                    | FieldType::Null => Value::Null,

                    FieldType::Text => Value::from("lorem ipsum".to_string()),
                    FieldType::Date => Value::from("2022-11-24"),
                };
                json!({ name: val })
            } else {
                json!({})
            }
        } else {
            json!({})
        }
        // Simple expression
    }

    fn generate_get_route(&self) -> ReferenceOr<PathItem> {
        let responses = Responses {
            responses: indexmap::indexmap! {
                StatusCode::Code(200) =>
                ReferenceOr::Item(create_reference_response(format!("Get by id {}", self.schema_name.to_owned()),format!("#/components/schemas/{}", self.get_singular_name())))
            },
            ..Default::default()
        };
        let get_operation = Some(Operation {
            tags: vec![format!("{}", self.schema_name.to_owned())],
            summary: Some("Fetch a single document record by primary key".to_owned()),
            description: Some(
                "Generated API to fetch a single record. Primary key specified will be used for lookup"
                    .to_owned(),
            ),
            operation_id: Some(format!("{}-by-id", self.schema_name.to_owned())),
            parameters: vec![ReferenceOr::Item(Parameter::Path {
                parameter_data: ParameterData {
                    name: "id".to_owned(),
                    description: Some(format!("Primary key of the document - {} ", self.endpoint.index.to_owned().unwrap().primary_key.join(", "))),
                    required: true,
                    format: ParameterSchemaOrContent::Schema(ReferenceOr::Item(Schema {
                        schema_data: SchemaData {
                            ..Default::default()
                        },
                        schema_kind: SchemaKind::Type(Type::Integer(Default::default())),
                    })),
                    deprecated: None,
                    example: None,
                    examples: IndexMap::new(),
                    explode: None,
                    extensions: IndexMap::new(),
                },
                style: PathStyle::Simple,
            })],
            responses,
            ..Default::default()
        });
        ReferenceOr::Item(PathItem {
            get: get_operation,
            ..Default::default()
        })
    }

    fn generate_list_route(&self) -> ReferenceOr<PathItem> {
        let responses = Responses {
            responses: indexmap::indexmap! {
                StatusCode::Code(200) => ReferenceOr::Item(create_reference_response(format!("A page array of {}", self.endpoint.name.to_owned()), format!("#/components/schemas/{}",self.get_plural_name())))
            },
            ..Default::default()
        };
        let operation = Some(Operation {
            tags: vec![format!("{}", self.schema_name.to_owned())],
            summary: Some("Fetch multiple documents in the default sort order".to_owned()),
            description: Some(
                "This is used when no filter expression or sort is needed.".to_owned(),
            ),
            operation_id: Some(format!("list-{}", self.endpoint.name.to_owned())),
            responses,
            ..Default::default()
        });
        ReferenceOr::Item(PathItem {
            get: operation,
            ..Default::default()
        })
    }

    fn generate_query_route(&self) -> ReferenceOr<PathItem> {
        let request_body = RequestBody {
            content: indexmap::indexmap! {
                "application/json".to_owned() => MediaType { example: Some(self.generate_query_example()), ..Default::default() }
            },
            required: true,
            ..Default::default()
        };
        let responses = Responses {
            responses: indexmap::indexmap! {
                StatusCode::Code(200) => ReferenceOr::Item(create_reference_response(format!("A page array of {}", self.endpoint.name.to_owned()), format!("#/components/schemas/{}", self.get_plural_name()) ))
            },
            ..Default::default()
        };
        let operation = Some(Operation {
            tags: vec![format!("{}", self.schema_name.to_owned())],
            summary: Some("Query documents based on an expression".to_owned()),
            description: Some(
                "Documents can be queried based on a simple or a composite expression".to_owned(),
            ),
            operation_id: Some(format!("query-{}", self.endpoint.name.to_owned())),
            request_body: Some(openapiv3::ReferenceOr::Item(request_body)),
            responses,
            ..Default::default()
        });
        ReferenceOr::Item(PathItem {
            post: operation,
            ..Default::default()
        })
    }

    fn _generate_available_paths(&self) -> Paths {
        let get_list = self.generate_list_route();
        let get_by_id_item = self.generate_get_route();
        let query_list = self.generate_query_route();
        let path_items = indexmap::indexmap! {
            self.endpoint.path.to_owned() => get_list,
            format!("{}/{}", self.endpoint.path.to_owned(), "{id}") => get_by_id_item,
            format!("{}/query", self.endpoint.path.to_owned()) => query_list
        };
        Paths {
            paths: path_items,
            ..Default::default()
        }
    }

    fn generate_component_schema(&self) -> Components {
        let generated_schema =
            convert_cache_to_oapi_schema(self.schema.to_owned(), self.schema_name.to_owned());

        let schemas = indexmap::indexmap! {
            self.get_singular_name() => ReferenceOr::Item(generated_schema),
            self.get_plural_name() => ReferenceOr::Item(Schema {
                        schema_data: SchemaData {
                            description: Some(format!("Array of {}", self.schema_name.to_owned())),
                            ..Default::default()
                        },
                        schema_kind: SchemaKind::Type(Type::Array(ArrayType {
                            items: Some(ReferenceOr::ref_(&format!("#/components/schemas/{}", self.get_singular_name()))),
                            min_items: None,
                            max_items: None,
                            unique_items: false,
                        })),
                    })
        };

        Components {
            schemas,
            ..Default::default()
        }
    }
}

impl OpenApiGenerator {
    pub fn generate_oas3(&self) -> Result<OpenAPI, GenerationError> {
        let component_schemas = self.generate_component_schema();
        let paths_available = self._generate_available_paths();

        let api = OpenAPI {
            openapi: "3.0.0".to_owned(),
            info: Info {
                title: self.endpoint.name.to_uppercase(),
                description: Some(format!(
                    "API documentation for {}. Powered by Dozer Data.",
                    self.endpoint.name.to_lowercase()
                )),
                version: "1.0.0".to_owned(),
                contact: create_contact_info(),
                ..Default::default()
            },
            tags: vec![Tag {
                name: self.schema_name.to_owned(),
                ..Default::default()
            }],
            servers: self
                .server_host
                .iter()
                .map(|host| Server {
                    url: host.to_owned(),
                    ..Default::default()
                })
                .collect(),
            paths: paths_available,
            components: Some(component_schemas),
            ..Default::default()
        };
        let tmp_dir =
            TempDir::new("generated").map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_dir.path().join("openapi.json"))
            .expect("Couldn't open file");
        serde_json::to_writer(f, &api).map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        Ok(api)
    }

    pub fn new(
        schema: dozer_types::types::Schema,
        secondary_indexes: Vec<IndexDefinition>,
        schema_name: String,
        endpoint: ApiEndpoint,
        server_host: Vec<String>,
    ) -> Self {
        Self {
            schema,
            secondary_indexes,
            endpoint,
            server_host,
            schema_name,
        }
    }
}
