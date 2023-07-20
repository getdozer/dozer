use crate::errors::GenerationError;
use crate::errors::GenerationError::ServiceNotFound;
use crate::generator::protoc::generator::{
    CountMethodDesc, DecimalDesc, DurationDesc, EventDesc, OnEventMethodDesc, PointDesc,
    QueryMethodDesc, RecordWithIdDesc, TokenMethodDesc, TokenResponseDesc,
};
use dozer_cache::dozer_log::schemas::BuildSchema;
use dozer_types::log::error;
use dozer_types::serde::{self, Deserialize, Serialize};
use dozer_types::types::{FieldType, Schema};
use handlebars::Handlebars;
use inflector::Inflector;
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};
use std::path::{Path, PathBuf};

use super::{CountResponseDesc, QueryResponseDesc, RecordDesc, ServiceDesc};

const POINT_TYPE_CLASS: &str = "dozer.types.PointType";
const DURATION_TYPE_CLASS: &str = "dozer.types.DurationType";
const DECIMAL_TYPE_CLASS: &str = "dozer.types.RustDecimal";
const TIMESTAMP_TYPE_CLASS: &str = "google.protobuf.Timestamp";
const JSON_TYPE_CLASS: &str = "google.protobuf.Value";

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
    schema: &'a BuildSchema,
    names: Names,
    folder_path: &'a Path,
}

impl<'a> ProtoGeneratorImpl<'a> {
    pub fn new(
        schema_name: &str,
        schema: &'a BuildSchema,
        folder_path: &'a Path,
    ) -> Result<Self, GenerationError> {
        let names = Names::new(schema_name, &schema.schema);
        let mut generator = Self {
            handlebars: Handlebars::new(),
            schema,
            names,
            folder_path,
        };
        generator.register_template()?;
        Ok(generator)
    }

    fn register_template(&mut self) -> Result<(), GenerationError> {
        let main_template = include_str!("template/proto.tmpl");
        self.handlebars
            .register_template_string("main", main_template)
            .map_err(|e| GenerationError::HandlebarsTemplate(Box::new(e)))?;
        Ok(())
    }

    fn props(&self) -> Vec<String> {
        self.schema
            .schema
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
        let type_need_import_libs = [TIMESTAMP_TYPE_CLASS, JSON_TYPE_CLASS];
        let mut libs_import: Vec<String> = self
            .schema
            .schema
            .fields
            .iter()
            .map(|field| convert_dozer_type_to_proto_type(field.to_owned().typ).unwrap())
            .filter(|proto_type| -> bool {
                type_need_import_libs.contains(&proto_type.to_owned().as_str())
            })
            .map(|proto_type| match proto_type.as_str() {
                TIMESTAMP_TYPE_CLASS => "google/protobuf/timestamp.proto".to_owned(),
                JSON_TYPE_CLASS => "google/protobuf/struct.proto".to_owned(),
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
            version_field_id: self.schema.schema.fields.len() + 1,
            enable_token: self.schema.enable_token,
            enable_on_event: self.schema.enable_on_event,
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

        let types_proto = include_str!("../../../../../dozer-types/protos/types.proto");

        let resource_proto = self.handlebars.render("main", &metadata)?;

        // Copy types proto file
        let types_path = self.folder_path.join("types.proto");
        std::fs::write(&types_path, types_proto)
            .map_err(|e| GenerationError::FailedToWriteToFile(types_path, e))?;

        let resource_path = self.folder_path.join(&self.names.proto_file_name);
        std::fs::write(&resource_path, &resource_proto)
            .map_err(|e| GenerationError::FailedToWriteToFile(resource_path.clone(), e))?;

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

        let record_desc_from_message =
            |message: MessageDescriptor| -> Result<RecordDesc, GenerationError> {
                let version_field = get_field(&message, "__dozer_record_version")?;

                if let Some(point_values) = descriptor.get_message_by_name(POINT_TYPE_CLASS) {
                    let pv = point_values;
                    if let Some(decimal_values) = descriptor.get_message_by_name(DECIMAL_TYPE_CLASS)
                    {
                        if let Some(dv) = descriptor.get_message_by_name(DURATION_TYPE_CLASS) {
                            let durv = dv;
                            Ok(RecordDesc {
                                message,
                                version_field,
                                point_field: PointDesc {
                                    message: pv.clone(),
                                    x: get_field(&pv, "x")?,
                                    y: get_field(&pv, "y")?,
                                },
                                decimal_field: DecimalDesc {
                                    message: decimal_values.clone(),
                                    scale: get_field(&decimal_values, "scale")?,
                                    lo: get_field(&decimal_values, "lo")?,
                                    mid: get_field(&decimal_values, "mid")?,
                                    hi: get_field(&decimal_values, "hi")?,
                                    negative: get_field(&decimal_values, "negative")?,
                                },
                                duration_field: DurationDesc {
                                    message: durv.clone(),
                                    value: get_field(&durv, "value")?,
                                    time_unit: get_field(&durv, "time_unit")?,
                                },
                            })
                        } else {
                            Err(ServiceNotFound(DURATION_TYPE_CLASS.to_string()))
                        }
                    } else {
                        Err(ServiceNotFound(DECIMAL_TYPE_CLASS.to_string()))
                    }
                } else {
                    Err(ServiceNotFound(POINT_TYPE_CLASS.to_string()))
                }
            };

        let names = Names::new(schema_name, &Schema::default());
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
                            actual: records_filed_kind,
                        });
                    };
                    let id_field = get_field(&record_with_id_message, "id")?;
                    let record_field = get_field(&record_with_id_message, "record")?;
                    let record_field_kind = record_field.kind();
                    let Kind::Message(record_message) = record_field_kind else {
                        return Err(GenerationError::ExpectedMessageField {
                            filed_name: record_field.full_name().to_string(),
                            actual: record_field_kind,
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
                    let new_id_field = get_field(&message, "new_id")?;
                    let old_field_kind = old_field.kind();
                    let Kind::Message(record_message) = old_field_kind else {
                        return Err(GenerationError::ExpectedMessageField {
                            filed_name: old_field.full_name().to_string(),
                            actual: old_field_kind,
                        });
                    };
                    on_event = Some(OnEventMethodDesc {
                        method,
                        response_desc: EventDesc {
                            message,
                            typ_field,
                            old_field,
                            new_field,
                            new_id_field,
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
            return Err(GenerationError::MissingCountMethod(
                service.full_name().to_string(),
            ));
        };
        let Some(query) = query else {
            return Err(GenerationError::MissingQueryMethod(
                service.full_name().to_string(),
            ));
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
        FieldType::U128 => Ok("string".to_owned()),
        FieldType::Int => Ok("int64".to_owned()),
        FieldType::I128 => Ok("string".to_owned()),
        FieldType::Float => Ok("double".to_owned()),
        FieldType::Boolean => Ok("bool".to_owned()),
        FieldType::String => Ok("string".to_owned()),
        FieldType::Text => Ok("string".to_owned()),
        FieldType::Binary => Ok("bytes".to_owned()),
        FieldType::Decimal => Ok(DECIMAL_TYPE_CLASS.to_owned()),
        FieldType::Timestamp => Ok(TIMESTAMP_TYPE_CLASS.to_owned()),
        FieldType::Date => Ok("string".to_owned()),
        FieldType::Json => Ok(JSON_TYPE_CLASS.to_owned()),
        FieldType::Point => Ok(POINT_TYPE_CLASS.to_owned()),
        FieldType::Duration => Ok(DURATION_TYPE_CLASS.to_owned()),
    }
}
