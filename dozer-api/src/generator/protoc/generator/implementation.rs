use crate::errors::GenerationError;
use crate::generator::protoc::generator::{
    CountMethodDesc, EventDesc, OnEventMethodDesc, QueryMethodDesc, RecordWithIdDesc,
    TokenMethodDesc, TokenResponseDesc,
};
use dozer_types::log::error;
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use dozer_types::serde::{self, Deserialize, Serialize};
use dozer_types::types::{FieldType, Schema};
use handlebars::Handlebars;
use inflector::Inflector;
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};
use std::path::{Path, PathBuf};

use super::{CountResponseDesc, QueryResponseDesc, RecordDesc, ServiceDesc};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
struct ProtoMetadata {
    import_libs: Vec<String>,
    package_name: String,
    lower_name: String,
    plural_pascal_name: String,
    pascal_name: String,
    props: Vec<String>,
    version_field_id: usize,
    enable_token: bool,
    enable_on_event: bool,
}

pub struct ProtoGeneratorImpl<'a> {
    handlebars: Handlebars<'a>,
    schema: dozer_types::types::Schema,
    names: Names,
    folder_path: &'a Path,
    security: &'a Option<ApiSecurity>,
    flags: &'a Option<Flags>,
}

impl<'a> ProtoGeneratorImpl<'a> {
    pub fn new(
        schema_name: &str,
        schema: Schema,
        folder_path: &'a Path,
        security: &'a Option<ApiSecurity>,
        flags: &'a Option<Flags>,
    ) -> Result<Self, GenerationError> {
        let names = Names::new(schema_name, &schema);
        let mut generator = Self {
            handlebars: Handlebars::new(),
            schema,
            names,
            folder_path,
            security,
            flags,
        };
        generator.register_template()?;
        Ok(generator)
    }

    fn register_template(&mut self) -> Result<(), GenerationError> {
        let main_template = include_str!("template/proto.tmpl");
        self.handlebars
            .register_template_string("main", main_template)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
        Ok(())
    }

    fn props(&self) -> Vec<String> {
        self.schema
            .fields
            .iter()
            .enumerate()
            .zip(&self.names.record_field_names)
            .map(|((idx, field), field_name)| -> String {
                let optional = if field.nullable { "optional " } else { "" };
                let proto_type = convert_dozer_type_to_proto_type(field.typ.to_owned()).unwrap();
                format!("{optional}{proto_type} {field_name} = {};", idx + 1)
            })
            .collect()
    }

    fn libs_by_type(&self) -> Result<Vec<String>, GenerationError> {
        let type_need_import_libs = ["google.protobuf.Timestamp"];
        let mut libs_import: Vec<String> = self
            .schema
            .fields
            .iter()
            .map(|field| convert_dozer_type_to_proto_type(field.to_owned().typ).unwrap())
            .filter(|proto_type| -> bool {
                type_need_import_libs.contains(&proto_type.to_owned().as_str())
            })
            .map(|proto_type| match proto_type.as_str() {
                "google.protobuf.Timestamp" => "google/protobuf/timestamp.proto".to_owned(),
                _ => "".to_owned(),
            })
            .collect();
        libs_import.push("types.proto".to_owned());
        libs_import.sort();
        libs_import.dedup();
        Ok(libs_import)
    }

    fn get_metadata(&self) -> Result<ProtoMetadata, GenerationError> {
        let import_libs: Vec<String> = self.libs_by_type()?;
        let metadata = ProtoMetadata {
            package_name: self.names.package_name.clone(),
            import_libs,
            lower_name: self.names.lower_name.clone(),
            plural_pascal_name: self.names.plural_pascal_name.clone(),
            pascal_name: self.names.pascal_name.clone(),
            props: self.props(),
            version_field_id: self.schema.fields.len() + 1,
            enable_token: self.security.is_some(),
            enable_on_event: self.flags.clone().unwrap_or_default().push_events,
        };
        Ok(metadata)
    }

    pub fn generate_proto(&self) -> Result<(String, PathBuf), GenerationError> {
        if !Path::new(&self.folder_path).exists() {
            return Err(GenerationError::DirPathNotExist(
                self.folder_path.to_path_buf(),
            ));
        }

        let metadata = self.get_metadata()?;

        let types_proto = include_str!("../../../../protos/types.proto");

        let resource_proto = self
            .handlebars
            .render("main", &metadata)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        // Copy types proto file
        let mut types_file = std::fs::File::create(self.folder_path.join("types.proto"))
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        let resource_path = self.folder_path.join(&self.names.proto_file_name);
        let mut resource_file = std::fs::File::create(resource_path.clone())
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        std::io::Write::write_all(&mut types_file, types_proto.as_bytes())
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        std::io::Write::write_all(&mut resource_file, resource_proto.as_bytes())
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        Ok((resource_proto, resource_path))
    }

    pub fn read(
        descriptor: &DescriptorPool,
        schema_name: &str,
    ) -> Result<ServiceDesc, GenerationError> {
        fn get_field(
            message: &MessageDescriptor,
            field_name: &str,
        ) -> Result<FieldDescriptor, GenerationError> {
            message
                .get_field_by_name(field_name)
                .ok_or_else(|| GenerationError::FieldNotFound {
                    message_name: message.name().to_string(),
                    field_name: field_name.to_string(),
                })
        }

        fn record_desc_from_message(
            message: MessageDescriptor,
        ) -> Result<RecordDesc, GenerationError> {
            let version_field = get_field(&message, "__dozer_record_version")?;
            Ok(RecordDesc {
                message,
                version_field,
            })
        }

        let names = Names::new(schema_name, &Schema::empty());
        let service_name = format!("{}.{}", &names.package_name, &names.plural_pascal_name);
        let service = descriptor
            .get_service_by_name(&service_name)
            .ok_or(GenerationError::ServiceNotFound(service_name))?;

        let mut count = None;
        let mut query = None;
        let mut on_event = None;
        let mut token = None;
        for method in service.methods() {
            match method.name() {
                "count" => {
                    let message = method.output();
                    let count_field = get_field(&message, "count")?;
                    count = Some(CountMethodDesc {
                        method,
                        response_desc: CountResponseDesc {
                            message,
                            count_field,
                        },
                    });
                }
                "query" => {
                    let message = method.output();
                    let records_field = get_field(&message, "records")?;
                    let records_filed_kind = records_field.kind();
                    let Kind::Message(record_with_id_message) = records_filed_kind else {
                        return Err(GenerationError::ExpectedMessageField {
                            filed_name: records_field.full_name().to_string(),
                            actual: records_filed_kind
                        });
                    };
                    let id_field = get_field(&record_with_id_message, "id")?;
                    let record_field = get_field(&record_with_id_message, "record")?;
                    let record_field_kind = record_field.kind();
                    let Kind::Message(record_message) = record_field_kind else {
                        return Err(GenerationError::ExpectedMessageField {
                            filed_name: record_field.full_name().to_string(),
                            actual: record_field_kind
                        });
                    };
                    query = Some(QueryMethodDesc {
                        method,
                        response_desc: QueryResponseDesc {
                            message,
                            records_field,
                            record_with_id_desc: RecordWithIdDesc {
                                message: record_with_id_message,
                                id_field,
                                record_field,
                                record_desc: record_desc_from_message(record_message)?,
                            },
                        },
                    });
                }
                "on_event" => {
                    let message = method.output();
                    let typ_field = get_field(&message, "typ")?;
                    let old_field = get_field(&message, "old")?;
                    let new_field = get_field(&message, "new")?;
                    let old_field_kind = old_field.kind();
                    let Kind::Message(record_message) = old_field_kind else {
                        return Err(GenerationError::ExpectedMessageField {
                            filed_name: old_field.full_name().to_string(),
                            actual: old_field_kind
                        });
                    };
                    on_event = Some(OnEventMethodDesc {
                        method,
                        response_desc: EventDesc {
                            message,
                            typ_field,
                            old_field,
                            new_field,
                            record_desc: record_desc_from_message(record_message)?,
                        },
                    });
                }
                "token" => {
                    let message = method.output();
                    let token_field = get_field(&message, "token")?;
                    token = Some(TokenMethodDesc {
                        method,
                        response_desc: TokenResponseDesc {
                            message,
                            token_field,
                        },
                    });
                }
                _ => {
                    return Err(GenerationError::UnexpectedMethod(
                        method.full_name().to_string(),
                    ))
                }
            }
        }

        let Some(count) = count else {
            return Err(GenerationError::MissingCountMethod(service.full_name().to_string()));
        };
        let Some(query) = query else {
            return Err(GenerationError::MissingQueryMethod(service.full_name().to_string()));
        };

        Ok(ServiceDesc {
            service,
            count,
            query,
            on_event,
            token,
        })
    }
}

struct Names {
    proto_file_name: String,
    package_name: String,
    lower_name: String,
    plural_pascal_name: String,
    pascal_name: String,
    record_field_names: Vec<String>,
}

impl Names {
    fn new(schema_name: &str, schema: &Schema) -> Self {
        if schema_name.contains('-') {
            error!("Name of the endpoint should not contain `-`.");
        }
        let schema_name = schema_name.replace(|c: char| !c.is_ascii_alphanumeric(), "_");

        let package_name = format!("dozer.generated.{schema_name}");
        let lower_name = schema_name.to_lowercase();
        let plural_pascal_name = schema_name.to_pascal_case().to_plural();
        let pascal_name = schema_name.to_pascal_case().to_singular();
        let record_field_names = schema
            .fields
            .iter()
            .map(|field| {
                if field.name.contains('-') {
                    error!("Name of the field should not contain `-`.");
                }
                field
                    .name
                    .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
            })
            .collect::<Vec<_>>();
        Self {
            proto_file_name: format!("{lower_name}.proto"),
            package_name,
            lower_name,
            plural_pascal_name,
            pascal_name,
            record_field_names,
        }
    }
}

fn convert_dozer_type_to_proto_type(field_type: FieldType) -> Result<String, GenerationError> {
    match field_type {
        FieldType::UInt => Ok("uint64".to_owned()),
        FieldType::Int => Ok("int64".to_owned()),
        FieldType::Float => Ok("double".to_owned()),
        FieldType::Boolean => Ok("bool".to_owned()),
        FieldType::String => Ok("string".to_owned()),
        FieldType::Text => Ok("string".to_owned()),
        FieldType::Decimal => Ok("double".to_owned()),
        FieldType::Timestamp => Ok("google.protobuf.Timestamp".to_owned()),
        FieldType::Date => Ok("string".to_owned()),
        FieldType::Bson => Ok("google.protobuf.Any".to_owned()),
        FieldType::Coord => Ok("dozer.types.CoordType".to_owned()),
        FieldType::Point => Ok("dozer.types.CoordType".to_owned()),
        _ => Err(GenerationError::DozerToProtoTypeNotSupported(format!(
            "{field_type:?}"
        ))),
    }
}
