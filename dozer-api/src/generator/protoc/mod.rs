use std::path::Path;

use dozer_cache::dozer_log::schemas::EndpointSchema;

use crate::errors::GenerationError;

use self::generator::ProtoGenerator;

pub mod generator;

pub fn generate_all<'a, I: IntoIterator<Item = (&'a str, &'a EndpointSchema)>>(
    proto_folder_path: &Path,
    descriptor_path: &Path,
    endpoints: I,
) -> Result<Vec<u8>, GenerationError> {
    let mut resources = Vec::new();

    for (endpoint_name, schema) in endpoints {
        let resource_name = ProtoGenerator::generate(proto_folder_path, endpoint_name, schema)?;
        resources.push(resource_name);
    }

    let common_resources = ProtoGenerator::copy_common(proto_folder_path)?;

    // Copy common service to be included in descriptor.
    resources.extend(common_resources);

    // Generate a descriptor based on all proto files generated.
    ProtoGenerator::generate_descriptor(proto_folder_path, descriptor_path, &resources)?;

    // Read descriptor data.
    let descriptor = std::fs::read(descriptor_path)
        .map_err(|e| GenerationError::FailedToReadProtoDescriptor(descriptor_path.into(), e))?;

    Ok(descriptor)
}

#[cfg(test)]
mod tests;
