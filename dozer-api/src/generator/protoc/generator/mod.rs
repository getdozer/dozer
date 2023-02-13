use crate::errors::GenerationError;
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use dozer_types::types::Schema;
use prost_reflect::DescriptorPool;
use std::path::Path;

use super::utils::{create_descriptor_set, get_proto_descriptor};

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

    pub fn generate_descriptor(
        folder_path: &Path,
        resources: Vec<String>,
    ) -> Result<ProtoResponse, GenerationError> {
        let descriptor_path = create_descriptor_set(folder_path, &resources)
            .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

        let (descriptor_bytes, descriptor) = get_proto_descriptor(&descriptor_path)?;

        Ok(ProtoResponse {
            descriptor,
            descriptor_bytes,
        })
    }

    pub fn read(folder_path: &Path) -> Result<ProtoResponse, GenerationError> {
        let descriptor_path = folder_path.join("file_descriptor_set.bin");
        let (descriptor_bytes, descriptor) = get_proto_descriptor(&descriptor_path)?;

        Ok(ProtoResponse {
            descriptor,
            descriptor_bytes,
        })
    }
}

mod implementation;
use implementation::ProtoGeneratorImpl;
