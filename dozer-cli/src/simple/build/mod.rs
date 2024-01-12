use dozer_cache::dozer_log::home_dir::{BuildId, HomeDir};

use crate::errors::BuildError;

mod contract;

pub use contract::{Contract, PipelineContract};

pub fn generate_protos(home_dir: &HomeDir, contract: &Contract) -> Result<(), BuildError> {
    let build_id = BuildId::first();
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
            .map(|(table_name, schema)| (table_name.as_str(), schema)),
    )?;

    Ok(())
}
