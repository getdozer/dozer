use crate::errors::GenerationError;
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use dozer_types::types::Schema;
use prost_reflect::DescriptorPool;
use std::{
    io,
    path::{Path, PathBuf},
};

use super::utils::get_proto_descriptor;

pub struct ProtoResponse {
    pub descriptor: DescriptorPool,
    pub descriptor_bytes: Vec<u8>,
}

pub struct ProtoGenerator;

impl ProtoGenerator {
    pub fn copy_common(folder_path: &Path) -> Result<Vec<String>, GenerationError> {
        let mut resource_names = vec![];
        let protos = vec![
            ("common", include_str!("../../../../protos/common.proto")),
            ("health", include_str!("../../../../protos/health.proto")),
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
        schema: Schema,
        security: &Option<ApiSecurity>,
        flags: &Option<Flags>,
    ) -> Result<(), GenerationError> {
        let generator = ProtoGeneratorImpl::new(schema_name, schema, folder_path, security, flags)?;
        generator.generate_proto()?;
        Ok(())
    }

    pub fn generate_descriptor<T: AsRef<str>>(
        folder_path: &Path,
        resources: &[T],
    ) -> Result<ProtoResponse, GenerationError> {
        let descriptor_path = descriptor_path(folder_path);
        create_descriptor_set(folder_path, &descriptor_path, resources)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        Self::read(folder_path)
    }

    pub fn read(folder_path: &Path) -> Result<ProtoResponse, GenerationError> {
        let descriptor_path = descriptor_path(folder_path);
        let (descriptor_bytes, descriptor) = get_proto_descriptor(&descriptor_path)?;

        Ok(ProtoResponse {
            descriptor,
            descriptor_bytes,
        })
    }
}

fn descriptor_path(folder_path: &Path) -> PathBuf {
    folder_path.join("file_descriptor_set.bin")
}

fn create_descriptor_set<T: AsRef<str>>(
    folder_path: &Path,
    descriptor_path: &Path,
    resources: &[T],
) -> Result<(), io::Error> {
    let resources: Vec<_> = resources
        .iter()
        .map(|r| folder_path.join(format!("{}.proto", r.as_ref())))
        .collect();

    let mut prost_build_config = prost_build::Config::new();
    prost_build_config.out_dir(folder_path.to_owned());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(descriptor_path)
        // .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .build_client(false)
        .build_server(false)
        .emit_rerun_if_changed(false)
        .out_dir(folder_path)
        .compile_with_config(prost_build_config, &resources, &[folder_path])?;
    Ok(())
}

mod implementation;
use implementation::ProtoGeneratorImpl;
