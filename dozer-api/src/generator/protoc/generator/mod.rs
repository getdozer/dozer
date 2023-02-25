use crate::errors::GenerationError;
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use dozer_types::types::Schema;
use prost_reflect::{
    DescriptorPool, FieldDescriptor, MessageDescriptor, MethodDescriptor, ServiceDescriptor,
};
use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct ServiceDesc {
    pub service: ServiceDescriptor,
    pub count: CountMethodDesc,
    pub query: QueryMethodDesc,
    pub on_event: Option<OnEventMethodDesc>,
    pub token: Option<TokenMethodDesc>,
}

#[derive(Debug, Clone)]
pub struct CountMethodDesc {
    pub method: MethodDescriptor,
    pub response_desc: CountResponseDesc,
}

#[derive(Debug, Clone)]
pub struct QueryMethodDesc {
    pub method: MethodDescriptor,
    pub response_desc: QueryResponseDesc,
}

#[derive(Debug, Clone)]
pub struct OnEventMethodDesc {
    pub method: MethodDescriptor,
    pub response_desc: EventDesc,
}

#[derive(Debug, Clone)]
pub struct TokenMethodDesc {
    pub method: MethodDescriptor,
    pub response_desc: TokenResponseDesc,
}

#[derive(Debug, Clone)]
pub struct CountResponseDesc {
    pub message: MessageDescriptor,
    pub count_field: FieldDescriptor,
}

#[derive(Debug, Clone)]
pub struct RecordDesc {
    pub message: MessageDescriptor,
    pub version_field: FieldDescriptor,
}

#[derive(Debug, Clone)]
pub struct RecordWithIdDesc {
    pub message: MessageDescriptor,
    pub id_field: FieldDescriptor,
    pub record_field: FieldDescriptor,
    pub record_desc: RecordDesc,
}

#[derive(Debug, Clone)]
pub struct QueryResponseDesc {
    pub message: MessageDescriptor,
    pub records_field: FieldDescriptor,
    pub record_with_id_desc: RecordWithIdDesc,
}

#[derive(Debug, Clone)]
pub struct EventDesc {
    pub message: MessageDescriptor,
    pub typ_field: FieldDescriptor,
    pub old_field: FieldDescriptor,
    pub new_field: FieldDescriptor,
    pub new_id_field: FieldDescriptor,
    pub record_desc: RecordDesc,
}

#[derive(Debug, Clone)]
pub struct TokenResponseDesc {
    pub message: MessageDescriptor,
    pub token_field: FieldDescriptor,
}

pub struct ProtoGenerator;

impl ProtoGenerator {
    pub fn copy_common(folder_path: &Path) -> Result<Vec<String>, GenerationError> {
        let mut resource_names = vec![];
        let protos = vec![
            (
                "types",
                include_str!("../../../../../dozer-types/protos/types.proto"),
            ),
            (
                "common",
                include_str!("../../../../../dozer-types/protos/common.proto"),
            ),
            (
                "health",
                include_str!("../../../../../dozer-types/protos/health.proto"),
            ),
        ];

        for (name, proto_str) in protos {
            let mut proto_file =
                std::fs::File::create(folder_path.join(format!("{name}.proto").as_str()))
                    .map_err(|e| GenerationError::InternalError(Box::new(e)))?;
            std::io::Write::write_all(&mut proto_file, proto_str.as_bytes())
                .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

            resource_names.push(name.to_string());
        }
        Ok(resource_names)
    }

    pub fn generate(
        folder_path: &Path,
        schema_name: &str,
        schema: &Schema,
        security: &Option<ApiSecurity>,
        flags: &Option<Flags>,
    ) -> Result<(), GenerationError> {
        let generator = ProtoGeneratorImpl::new(schema_name, schema, folder_path, security, flags)?;
        generator.generate_proto()?;
        Ok(())
    }

    pub fn generate_descriptor<T: AsRef<str>>(
        proto_folder_path: &Path,
        descriptor_path: &Path,
        resources: &[T],
    ) -> Result<(), GenerationError> {
        create_descriptor_set(proto_folder_path, descriptor_path, resources)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))
    }

    pub fn read_descriptor_bytes(descriptor_path: &Path) -> Result<Vec<u8>, GenerationError> {
        read_file_as_byte(descriptor_path).map_err(|e| GenerationError::InternalError(Box::new(e)))
    }

    pub fn read_schema(
        descriptor_path: &Path,
        schema_name: &str,
    ) -> Result<ServiceDesc, GenerationError> {
        let descriptor_bytes = Self::read_descriptor_bytes(descriptor_path)?;
        let descriptor = DescriptorPool::decode(descriptor_bytes.as_slice())
            .map_err(GenerationError::ProtoDescriptorError)?;
        ProtoGeneratorImpl::read(&descriptor, schema_name)
    }

    pub fn descriptor_path(folder_path: &Path) -> PathBuf {
        folder_path.join("file_descriptor_set.bin")
    }
}

fn create_descriptor_set<T: AsRef<str>>(
    proto_folder_path: &Path,
    descriptor_path: &Path,
    resources: &[T],
) -> Result<(), io::Error> {
    let resources: Vec<_> = resources
        .iter()
        .map(|r| proto_folder_path.join(format!("{}.proto", r.as_ref())))
        .collect();

    let mut prost_build_config = prost_build::Config::new();
    prost_build_config.out_dir(proto_folder_path);
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(descriptor_path)
        // .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .build_client(false)
        .build_server(false)
        .emit_rerun_if_changed(false)
        .out_dir(proto_folder_path)
        .compile_with_config(prost_build_config, &resources, &[proto_folder_path])?;
    Ok(())
}

fn read_file_as_byte(path: &Path) -> Result<Vec<u8>, io::Error> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(buffer)
}

mod implementation;
use implementation::ProtoGeneratorImpl;
