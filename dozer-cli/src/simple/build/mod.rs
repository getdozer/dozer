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
        generate_protos(home_dir, build_id, contract)?;
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

fn generate_protos(
    home_dir: &HomeDir,
    build_id: BuildId,
    contract: &Contract,
) -> Result<(), BuildError> {
    let build_path = home_dir
        .create_build_dir_all(build_id)
        .map_err(|(path, error)| BuildError::FileSystem(path.into(), error))?;
    let proto_folder_path = build_path.contracts_dir.as_ref();
    let descriptor_path = build_path.descriptor_path.as_ref();

    dozer_api::generator::protoc::generate_all(
        proto_folder_path,
        descriptor_path,
        contract
            .endpoints
            .iter()
            .map(|(endpoint_name, schema)| (endpoint_name.as_str(), schema)),
    )?;

    Ok(())
}
