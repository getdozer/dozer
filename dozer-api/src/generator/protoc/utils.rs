use std::{
    fs::File,
    io::{self, BufReader, Read},
};

use prost_reflect::DescriptorPool;

use crate::errors::GenerationError;

pub fn get_proto_descriptor(
    descriptor_path: String,
) -> Result<(Vec<u8>, DescriptorPool), GenerationError> {
    let descriptor_bytes = read_file_as_byte(descriptor_path)
        .map_err(|e| GenerationError::InternalError(Box::new(e)))?;

    let descriptor = DescriptorPool::decode(descriptor_bytes.as_slice())
        .map_err(GenerationError::ProtoDescriptorError)?;

    Ok((descriptor_bytes, descriptor))
}

pub fn read_file_as_byte(path: String) -> Result<Vec<u8>, io::Error> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(buffer)
}

pub fn create_descriptor_set(
    folder_path: String,
    resources: &[String],
) -> Result<String, io::Error> {
    let my_path_descriptor = format!("{}/file_descriptor_set.bin", folder_path);

    let resources: Vec<String> = resources
        .iter()
        .map(|r| format!("{}/{}.proto", folder_path, r))
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
        .out_dir(folder_path.clone())
        .compile_with_config(prost_build_config, &resources, &[folder_path])?;
    Ok(my_path_descriptor)
}
