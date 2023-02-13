use crate::errors::GenerationError;
use dozer_types::log::error;
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use dozer_types::serde::{self, Deserialize, Serialize};
use dozer_types::types::{FieldType, Schema};
use handlebars::Handlebars;
use inflector::Inflector;
use std::fmt::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
struct ProtoMetadata {
    import_libs: Vec<String>,
    messages: Vec<RPCMessage>,
    package_name: String,
    lower_name: String,
    plural_pascal_name: String,
    pascal_name: String,
    enable_token: bool,
    enable_on_event: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
struct RPCMessage {
    name: String,
    props: Vec<String>,
}

pub struct ProtoGeneratorImpl<'a> {
    handlebars: Handlebars<'a>,
    schema: dozer_types::types::Schema,
    schema_name: String,
    folder_path: &'a Path,
    security: &'a Option<ApiSecurity>,
    flags: &'a Option<Flags>,
}

fn safe_name(name: &str) -> String {
    if name.contains('-') {
        error!("Name of the endpoint should not contains `-`.");
    }
    name.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
}

impl<'a> ProtoGeneratorImpl<'a> {
    pub fn new(
        schema_name: &str,
        schema: Schema,
        folder_path: &'a Path,
        security: &'a Option<ApiSecurity>,
        flags: &'a Option<Flags>,
    ) -> Result<Self, GenerationError> {
        let schema_name = safe_name(schema_name);
        let mut generator = Self {
            handlebars: Handlebars::new(),
            schema,
            schema_name,
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

    fn resource_message(&self) -> RPCMessage {
        let props_message: Vec<String> = self
            .schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| -> String {
                let mut result = "".to_owned();
                if field.nullable {
                    result.push_str("optional ");
                }
                let proto_type = convert_dozer_type_to_proto_type(field.typ.to_owned()).unwrap();
                let _ = writeln!(
                    result,
                    "{} {} = {}; ",
                    proto_type,
                    safe_name(&field.name),
                    idx + 1
                );
                result
            })
            .collect();

        RPCMessage {
            name: self.schema_name.to_pascal_case().to_singular(),
            props: props_message,
        }
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
        let package_name = format!("dozer.generated.{}", self.schema_name);

        let messages = vec![self.resource_message()];

        let import_libs: Vec<String> = self.libs_by_type()?;
        let metadata = ProtoMetadata {
            package_name,
            messages,
            import_libs,
            lower_name: self.schema_name.to_lowercase(),
            plural_pascal_name: self.schema_name.to_pascal_case().to_plural(),
            pascal_name: self.schema_name.to_pascal_case().to_singular(),
            enable_token: self.security.is_some(),
            enable_on_event: self.flags.to_owned().unwrap_or_default().push_events,
        };
        Ok(metadata)
    }

    pub fn generate_proto(&self) -> Result<(String, PathBuf), GenerationError> {
        if !Path::new(&self.folder_path).exists() {
            return Err(GenerationError::DirPathNotExist);
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

        let resource_path = self
            .folder_path
            .join(format!("{}.proto", self.schema_name.to_lowercase()));
        let mut resource_file = std::fs::File::create(resource_path.clone())
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        std::io::Write::write_all(&mut types_file, types_proto.as_bytes())
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        std::io::Write::write_all(&mut resource_file, resource_proto.as_bytes())
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        Ok((resource_proto, resource_path))
    }
}

fn convert_dozer_type_to_proto_type(field_type: FieldType) -> Result<String, GenerationError> {
    match field_type {
        FieldType::UInt => Ok("uint64".to_owned()),
        FieldType::Int => Ok("int64".to_owned()),
        FieldType::Float => Ok("double".to_owned()),
        FieldType::Boolean => Ok("bool".to_owned()),
        FieldType::String => Ok("string".to_owned()),
        FieldType::Decimal => Ok("double".to_owned()),
        FieldType::Timestamp => Ok("google.protobuf.Timestamp".to_owned()),
        FieldType::Date => Ok("string".to_owned()),
        FieldType::Bson => Ok("google.protobuf.Any".to_owned()),
        _ => Err(GenerationError::DozerToProtoTypeNotSupported(format!(
            "{field_type:?}"
        ))),
    }
}
