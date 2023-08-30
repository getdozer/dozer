use dozer_api::generator::protoc::generator::ProtoGenerator;
use dozer_cache::dozer_log::{
    home_dir::{BuildId, HomeDir},
    replication::create_data_storage,
    storage::Storage,
};
use dozer_types::{log::info, models::app_config::DataStorage};
use futures::future::try_join_all;

use crate::errors::BuildError;

mod contract;

pub use contract::{Contract, PipelineContract};

pub async fn build(
    home_dir: &HomeDir,
    contract: &Contract,
    storage_config: &DataStorage,
) -> Result<(), BuildError> {
    if let Some(build_id) = needs_build(home_dir, contract, storage_config).await? {
        let build_name = build_id.name().to_string();
        create_build(home_dir, build_id, contract)?;
        info!("Created new build {build_name}");
    } else {
        info!("Building not needed");
    }
    Ok(())
}

async fn needs_build(
    home_dir: &HomeDir,
    contract: &Contract,
    storage_config: &DataStorage,
) -> Result<Option<BuildId>, BuildError> {
    let build_path = home_dir
        .find_latest_build_path()
        .map_err(|(path, error)| BuildError::FileSystem(path.into(), error))?;
    let Some(build_path) = build_path else {
        return Ok(Some(BuildId::first()));
    };

    let mut futures = vec![];
    for endpoint in contract.endpoints.keys() {
        let endpoint_path = build_path.get_endpoint_path(endpoint);
        let log_dir = build_path
            .data_dir
            .join(endpoint_path.log_dir_relative_to_data_dir);
        let (storage, prefix) = create_data_storage(storage_config.clone(), log_dir.into()).await?;
        futures.push(is_empty(storage, prefix));
    }
    if !try_join_all(futures)
        .await?
        .into_iter()
        .all(|is_empty| is_empty)
    {
        return Ok(Some(build_path.id.next()));
    }

    let existing_contract = Contract::deserialize(&build_path)?;
    for (endpoint, schema) in &contract.endpoints {
        if let Some(existing_schema) = existing_contract.endpoints.get(endpoint) {
            if schema == existing_schema {
                continue;
            }
        } else {
            return Ok(Some(build_path.id.next()));
        }
    }
    Ok(None)
}

async fn is_empty(storage: Box<dyn Storage>, prefix: String) -> Result<bool, BuildError> {
    let objects = storage.list_objects(prefix, None).await?;
    Ok(objects.objects.is_empty())
}

fn create_build(
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
        ProtoGenerator::generate(proto_folder_path, endpoint_name, schema)?;
        resources.push(endpoint_name.clone());
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

    contract.serialize(&build_path)?;

    Ok(())
}
