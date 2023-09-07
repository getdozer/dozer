use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_cache::dozer_log::home_dir::{BuildId, HomeDir};
use dozer_types::log::info;

use crate::errors::BuildError;

mod contract;

pub use contract::{Contract, PipelineContract};

pub async fn build(
    home_dir: &HomeDir,
    contract: &Contract,
    existing_contract: Option<&Contract>,
) -> Result<(), BuildError> {
    if let Some(build_id) = new_build_id(home_dir, contract, existing_contract).await? {
        let build_name = build_id.name().to_string();
        build_endpoint_protos(home_dir, build_id, contract)?;
        info!("Created new build {build_name}");
    } else {
        info!("Building not needed");
    }
    Ok(())
}

async fn new_build_id(
    home_dir: &HomeDir,
    contract: &Contract,
    existing_contract: Option<&Contract>,
) -> Result<Option<BuildId>, BuildError> {
    let build_path = home_dir
        .find_latest_build_path()
        .map_err(|(path, error)| BuildError::FileSystem(path.into(), error))?;
    let Some(build_path) = build_path else {
        return Ok(Some(BuildId::first()));
    };

    let Some(existing_contract) = existing_contract else {
        return Ok(Some(build_path.id.next()));
    };

    for (endpoint, schema) in &contract.endpoints {
        if let Some(existing_schema) = existing_contract.endpoints.get(endpoint) {
            if schema == existing_schema {
                continue;
            }
        }
        return Ok(Some(build_path.id.next()));
    }
    Ok(None)
}

fn build_endpoint_protos(
    home_dir: &HomeDir,
    build_id: BuildId,
    contract: &Contract,
) -> Result<(), BuildError> {
    let build_path = home_dir
        .create_build_dir_all(build_id)
        .map_err(|(path, error)| BuildError::FileSystem(path.into(), error))?;

    let mut resources = Vec::new();

    let proto_folder_path = build_path.contracts_dir.as_ref();
    for (endpoint_name, schema) in &contract.endpoints {
        let resource_name = ProtoGenerator::generate(proto_folder_path, endpoint_name, schema)?;
        resources.push(resource_name);
    }

    let common_resources = ProtoGenerator::copy_common(proto_folder_path)?;

    // Copy common service to be included in descriptor.
    resources.extend(common_resources);

    // Generate a descriptor based on all proto files generated within sink.
    ProtoGenerator::generate_descriptor(
        proto_folder_path,
        build_path.descriptor_path.as_ref(),
        &resources,
    )?;

    Ok(())
}
