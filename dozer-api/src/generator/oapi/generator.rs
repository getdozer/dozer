use super::utils::{convert_cache_to_oapi_schema, convert_cache_type_to_schema_type};
use anyhow::Result;
use dozer_types::models::api_endpoint::ApiEndpoint;
use indexmap::IndexMap;
use openapiv3::*;

pub struct OpenApiGenerator {
    cache_schema: dozer_types::types::Schema,
    endpoint: ApiEndpoint,
}
impl OpenApiGenerator {
    fn generate_path_and_oapi_model(&self) -> Result<(Paths, Option<Components>)> {
        let mut single_name = self.endpoint.name.to_owned();
        single_name.pop();

        let plural_name = self.endpoint.name.to_owned();

        todo!()
    }
}

impl OpenApiGenerator {
    pub fn generate_oas3(&self) -> Result<OpenAPI> {
        let generated_schema = convert_cache_to_oapi_schema(
            self.cache_schema.to_owned(),
            self.endpoint.name.to_owned(),
        )?;
        // single
        let mut single_name = self.endpoint.name.to_owned();
        single_name.pop();
        let plural_name = self.endpoint.name.to_owned();
        let schemas = indexmap::indexmap! {
            single_name.to_owned() => ReferenceOr::Item(generated_schema),
            plural_name.to_owned() => ReferenceOr::Item(Schema {
                        schema_data: SchemaData {
                            description: Some(format!("Array of {}", single_name.to_owned())),
                            ..Default::default()
                        },
                        schema_kind: SchemaKind::Type(Type::Array(ArrayType {
                            items: Some(ReferenceOr::Reference {
                                reference: format!("#/components/schemas/{}", single_name),
                            }),
                            min_items: None,
                            max_items: None,
                            unique_items: false,
                        })),
                    })
        };
        let component_schemas = Some(Components {
            schemas: schemas,
            ..Default::default()
        });

        let post_responses = indexmap::indexmap! {
            StatusCode::Code(200) =>      ReferenceOr::Item(Response {
                        description: format!("A page array of {}", self.endpoint.name.to_owned()),
                        content: indexmap::indexmap!{
                            "application/json".to_owned() => MediaType { schema: Some(ReferenceOr::Reference { reference: format!("#/components/schemas/{}", self.endpoint.name.to_owned()) }), ..Default::default() }
                        },
                        ..Default::default()
                    })
        };
        let mut path_items: IndexMap<String, ReferenceOr<PathItem>> = IndexMap::new();
        path_items.insert(
            self.endpoint.path.to_owned(),
            ReferenceOr::Item(PathItem {
                post: Some(Operation {
                    tags: vec![self.endpoint.name.to_owned()],
                    summary: Some("summary".to_owned()),
                    description: Some("some description".to_owned()),
                    operation_id: Some(format!("list-{}", self.endpoint.name.to_owned())),
                    parameters: vec![],
                    //request_body: Some(),
                    responses: Responses {
                        responses: post_responses,
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );

        let get_responses = indexmap::indexmap! {
            StatusCode::Code(200) =>
            ReferenceOr::Item(Response {
                description: format!("Get by id {}", single_name.to_owned()),
                content: indexmap::indexmap!{
                    "application/json".to_owned() => MediaType { schema: Some(ReferenceOr::Reference { reference: format!("#/components/schemas/{}", single_name.to_owned()) }), ..Default::default() }
                },
                ..Default::default()
            })
        };
        path_items.insert(
            format!("{}/{}", self.endpoint.path.to_owned(), "{id}"),
            ReferenceOr::Item(PathItem {
                get: Some(Operation {
                    tags: vec![self.endpoint.name.to_owned()],
                    summary: Some("summary".to_owned()),
                    description: Some("some description".to_owned()),
                    operation_id: Some(format!("{}-by-id", single_name.to_owned())),
                    parameters: vec![ReferenceOr::Item(Parameter::Path {
                        parameter_data: ParameterData {
                            name: "id".to_owned(),
                            description: Some(format!("Id of {} to fetch", single_name).to_owned()),
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
                    responses: Responses {
                        responses: get_responses,
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            }),
        );

        let paths_available: Paths = Paths {
            paths: path_items,
            ..Default::default()
        };

        let api = OpenAPI {
            openapi: "3.0.0".to_owned(),
            info: Info {
                title: self.endpoint.name.to_uppercase(),
                license: None,
                description: Some(self.endpoint.name.to_lowercase().to_owned()),
                version: "1.0.0".to_owned(),
                contact: Some(Contact {
                    name: Some("Dozer-Team".to_owned()),
                    url: Some("https://getdozer.io".to_owned()),
                    email: Some("api@getdozer.io".to_owned()),
                    extensions: Default::default(),
                }),
                extensions: Default::default(),
                ..Default::default()
            },
            tags: vec![Tag {
                name: self.endpoint.name.to_owned(),
                ..Default::default()
            }],
            servers: vec![Server {
                url: "http://localhost:8080".to_owned(),
                ..Default::default()
            }],
            paths: paths_available,
            components: component_schemas,
            ..Default::default()
        };
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open("/Users/anhthu/Dozer/dozer/dozer-api/test_generate.yml")
            .expect("Couldn't open file");
        serde_yaml::to_writer(f, &api).unwrap();
        Ok(api)
    }

    pub fn new(schema: dozer_types::types::Schema, endpoint: ApiEndpoint) -> Result<Self> {
        let openapi_generator = Self {
            cache_schema: schema,
            endpoint: endpoint,
        };
        Ok(openapi_generator)
    }
}
