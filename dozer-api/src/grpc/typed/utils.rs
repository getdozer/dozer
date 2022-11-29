use crate::errors::GRPCError;
use dozer_cache::errors::CacheError;
use prost_reflect::DescriptorPool;
use std::{
    fs::File,
    io::{self, BufReader, Read},
};
use tonic::{Code, Status};

//https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/DescriptorProtos.FieldDescriptorProto.Type
pub fn get_proto_descriptor(descriptor_dir: String) -> Result<DescriptorPool, GRPCError> {
    let descriptor_set_dir = descriptor_dir;
    let buffer =
        read_file_as_byte(descriptor_set_dir).map_err(|e| GRPCError::InternalError(Box::new(e)))?;
    let my_array_byte = buffer.as_slice();
    let pool2 = DescriptorPool::decode(my_array_byte)
        .map_err(|e| GRPCError::ProtoDescriptorError(e.to_string()))?;
    Ok(pool2)
}

pub fn read_file_as_byte(path: String) -> Result<Vec<u8>, io::Error> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(buffer)
}

pub fn create_descriptor_set(
    proto_folder: &str,
    proto_file_name: &str,
) -> Result<String, io::Error> {
    let proto_file_path = format!("{}/{}", proto_folder.to_owned(), proto_file_name.to_owned());
    let my_path_descriptor = format!("{}/file_descriptor_set.bin", proto_folder.to_owned());
    let mut prost_build_config = prost_build::Config::new();
    prost_build_config.out_dir(proto_folder.to_owned());
    tonic_build::configure()
        .file_descriptor_set_path(&my_path_descriptor)
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .build_client(false)
        .build_server(false)
        .out_dir(proto_folder)
        .compile_with_config(prost_build_config, &[proto_file_path], &[proto_folder])?;
    Ok(my_path_descriptor)
}

pub fn from_cache_error(error: CacheError) -> Status {
    Status::new(Code::Internal, error.to_string())
}
