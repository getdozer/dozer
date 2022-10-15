use super::utils::{
    convert_cache_to_oapi_schema, create_contact_info, create_reference_response,
    generate_filter_expression_schema,
};
use anyhow::Result;
use dozer_types::models::api_endpoint::ApiEndpoint;
use indexmap::IndexMap;
use openapiv3::*;

pub struct OpenApiGenerator {
    schema: dozer_types::types::Schema,
    schema_name: String,
    endpoint: ApiEndpoint,
    server_host: Vec<String>,
}
impl OpenApiGenerator {
    fn _generate_get_by_id(&self) -> Result<ReferenceOr<PathItem>> {
        let responses = Responses {
            responses: indexmap::indexmap! {
                StatusCode::Code(200) =>
                ReferenceOr::Item(create_reference_response(format!("Get by id {}", self.schema_name.to_owned()),format!("#/components/schemas/{}", self.schema_name.to_owned())))

            },
            ..Default::default()
        };
        let get_operation = Some(Operation {
            tags: vec![format!("{}s", self.schema_name.to_owned())],
            summary: Some("summary".to_owned()),
            description: Some("some description".to_owned()),
            operation_id: Some(format!("{}-by-id", self.schema_name.to_owned())),
            parameters: vec![ReferenceOr::Item(Parameter::Path {
                parameter_data: ParameterData {
                    name: "id".to_owned(),
                    description: Some(format!("Id of {} to fetch", self.schema_name.to_owned())),
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
        Ok(ReferenceOr::Item(PathItem {
            get: get_operation,
            ..Default::default()
        }))
    }

    fn _generate_get_list(&self) -> Result<ReferenceOr<PathItem>> {
        let responses = Responses {
            responses: indexmap::indexmap! {
                StatusCode::Code(200) => ReferenceOr::Item(create_reference_response(format!("A page array of {}", self.endpoint.name.to_owned()), format!("#/components/schemas/{}s",self.schema_name.to_owned())))
            },
            ..Default::default()
        };
        let operation = Some(Operation {
            tags: vec![format!("{}s", self.schema_name.to_owned())],
            summary: Some("summary".to_owned()),
            description: Some("some description".to_owned()),
            operation_id: Some(format!("list-{}", self.endpoint.name.to_owned())),
            responses,
            ..Default::default()
        });
        Ok(ReferenceOr::Item(PathItem {
            get: operation,
            ..Default::default()
        }))
    }

    fn _generate_list_query(&self) -> Result<ReferenceOr<PathItem>> {
        let request_body = RequestBody {
            content: indexmap::indexmap! {
                "application/json".to_owned() => MediaType { schema: Some(ReferenceOr::ref_("#/components/schemas/filter-expression")), ..Default::default() }
            },
            required: true,
            ..Default::default()
        };
        let responses = Responses {
            responses: indexmap::indexmap! {
                StatusCode::Code(200) => ReferenceOr::Item(create_reference_response(format!("A page array of {}", self.endpoint.name.to_owned()), format!("#/components/schemas/{}s", self.schema_name.to_owned()) ))
            },
            ..Default::default()
        };
        let operation = Some(Operation {
            tags: vec![format!("{}s", self.schema_name.to_owned())],
            summary: Some("summary".to_owned()),
            description: Some("some description".to_owned()),
            operation_id: Some(format!("query-{}", self.endpoint.name.to_owned())),
            request_body: Some(openapiv3::ReferenceOr::Item(request_body)),
            responses,
            ..Default::default()
        });
        Ok(ReferenceOr::Item(PathItem {
            post: operation,
            ..Default::default()
        }))
    }

    fn _generate_available_paths(&self) -> Result<Paths> {
        let get_list = self._generate_get_list()?;
        let get_by_id_item = self._generate_get_by_id()?;
        let query_list = self._generate_list_query()?;
        let path_items = indexmap::indexmap! {
            self.endpoint.path.to_owned() => get_list,
            format!("{}/{}", self.endpoint.path.to_owned(), "{id}") => get_by_id_item,
            format!("{}/query", self.endpoint.path.to_owned()) => query_list
        };
        let paths_available: Paths = Paths {
            paths: path_items,
            ..Default::default()
        };
        Ok(paths_available)
    }

    fn _generate_component_schema(&self) -> Result<Option<Components>> {
        let plural_name = format!("{}s", self.schema_name.to_owned());
        let generated_schema =
            convert_cache_to_oapi_schema(self.schema.to_owned(), self.schema_name.to_owned())?;
        let mut schemas = indexmap::indexmap! {
            self.schema_name.to_owned() => ReferenceOr::Item(generated_schema),
            plural_name => ReferenceOr::Item(Schema {
                        schema_data: SchemaData {
                            description: Some(format!("Array of {}", self.schema_name.to_owned())),
                            ..Default::default()
                        },
                        schema_kind: SchemaKind::Type(Type::Array(ArrayType {
                            items: Some(ReferenceOr::ref_(&format!("#/components/schemas/{}", self.schema_name.to_owned()))),
                            min_items: None,
                            max_items: None,
                            unique_items: false,
                        })),
                    })
        };
        let filter_schemas = generate_filter_expression_schema()?;
        for filter_schema in filter_schemas {
            schemas.insert(
                filter_schema.0.to_string(),
                ReferenceOr::Item(filter_schema.1),
            );
        }
        let component_schemas = Some(Components {
            schemas,
            ..Default::default()
        });
        Ok(component_schemas)
    }
}

impl OpenApiGenerator {
    pub fn generate_oas3(&self, path: Option<String>) -> Result<OpenAPI> {
        let component_schemas = self._generate_component_schema()?;
        let paths_available = self._generate_available_paths()?;

        let api = OpenAPI {
            openapi: "3.0.0".to_owned(),
            info: Info {
                title: self.endpoint.name.to_uppercase(),
                description: Some(format!(
                    "API documentation for {}",
                    self.endpoint.name.to_lowercase()
                )),
                version: "1.0.0".to_owned(),
                contact: create_contact_info(),
                ..Default::default()
            },
            tags: vec![Tag {
                name: format!("{}s", self.schema_name.to_owned()),
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
            components: component_schemas,
            ..Default::default()
        };
        if let Some(path) = path {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(path)
                .expect("Couldn't open file");
            serde_yaml::to_writer(f, &api).unwrap();
        }
        Ok(api)
    }

    pub fn new(
        schema: dozer_types::types::Schema,
        schema_name: String,
        endpoint: ApiEndpoint,
        server_host: Vec<String>,
    ) -> Result<Self> {
        let openapi_generator = Self {
            schema,
            endpoint,
            server_host,
            schema_name,
        };
        Ok(openapi_generator)
    }
}
