use crate::errors::GenerationError;
use dozer_cache::dozer_log::schemas::MigrationSchema;
use prost_reflect::{
    DescriptorPool, FieldDescriptor, MessageDescriptor, MethodDescriptor, ServiceDescriptor,
};
use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
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
    pub point_field: PointDesc,
    pub decimal_field: DecimalDesc,
    pub duration_field: DurationDesc,
}

#[derive(Debug, Clone)]
pub struct PointDesc {
    pub message: MessageDescriptor,
    pub x: FieldDescriptor,
    pub y: FieldDescriptor,
}

#[derive(Debug, Clone)]
pub struct DurationDesc {
    pub message: MessageDescriptor,
    pub value: FieldDescriptor,
    pub time_unit: FieldDescriptor,
}

#[derive(Debug, Clone)]
pub struct DecimalDesc {
    pub message: MessageDescriptor,
    pub flags: FieldDescriptor,
    pub lo: FieldDescriptor,
    pub mid: FieldDescriptor,
    pub hi: FieldDescriptor,
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
            (
                "auth",
                include_str!("../../../../../dozer-types/protos/auth.proto"),
            ),
        ];

        for (name, proto_str) in protos {
            let proto_path = folder_path.join(format!("{name}.proto"));
            std::fs::write(&proto_path, proto_str)
                .map_err(|e| GenerationError::FailedToWriteToFile(proto_path, e))?;

            resource_names.push(name.to_string());
        }
        Ok(resource_names)
    }

    pub fn generate(
        folder_path: &Path,
        schema_name: &str,
        schema: &MigrationSchema,
    ) -> Result<(), GenerationError> {
        let generator = ProtoGeneratorImpl::new(schema_name, schema, folder_path)?;
        generator.generate_proto()?;
        Ok(())
    }

    pub fn generate_descriptor<T: AsRef<str>>(
        proto_folder_path: &Path,
        descriptor_path: &Path,
        resources: &[T],
    ) -> Result<(), GenerationError> {
        create_descriptor_set(proto_folder_path, descriptor_path, resources)
            .map_err(GenerationError::FailedToCreateProtoDescriptor)
    }

    pub fn read_descriptor_bytes(descriptor_path: &Path) -> Result<Vec<u8>, GenerationError> {
        read_file_as_byte(descriptor_path).map_err(|e| {
            GenerationError::FailedToReadProtoDescriptor(descriptor_path.to_path_buf(), e)
        })
    }

    pub fn read_schema(
        descriptor_path: &Path,
        schema_name: &str,
    ) -> Result<ServiceDesc, GenerationError> {
        let descriptor_bytes = Self::read_descriptor_bytes(descriptor_path)?;
        let descriptor = DescriptorPool::decode(descriptor_bytes.as_slice())
            .map_err(GenerationError::FailedToDecodeProtoDescriptor)?;
        ProtoGeneratorImpl::read(&descriptor, schema_name)
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
