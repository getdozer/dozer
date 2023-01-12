use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

use prost_reflect::DescriptorPool;

use crate::errors::GenerationError;

pub fn get_proto_descriptor(
    descriptor_path: &Path,
) -> Result<(Vec<u8>, DescriptorPool), GenerationError> {
    let descriptor_bytes = read_file_as_byte(descriptor_path)
        .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

    let descriptor = DescriptorPool::decode(descriptor_bytes.as_slice())
        .map_err(GenerationError::ProtoDescriptorError)?;

    Ok((descriptor_bytes, descriptor))
}

pub fn read_file_as_byte(path: &Path) -> Result<Vec<u8>, io::Error> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(buffer)
}

pub fn create_descriptor_set(
    folder_path: &Path,
    resources: &[String],
) -> Result<PathBuf, io::Error> {
    let my_path_descriptor = folder_path.join("file_descriptor_set.bin");

    let resources: Vec<_> = resources
        .iter()
        .map(|r| folder_path.join(format!("{}.proto", r)))
        .collect();

    let mut prost_build_config = prost_build::Config::new();
    prost_build_config.out_dir(folder_path.to_owned());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(&my_path_descriptor)
        // .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .build_client(false)
        .build_server(false)
        .emit_rerun_if_changed(false)
        .out_dir(folder_path)
        .compile_with_config(prost_build_config, &resources, &[folder_path])?;
    Ok(my_path_descriptor)
}
