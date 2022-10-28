use crate::errors::{GRPCError, GenerationError};
use dozer_cache::cache::expression::QueryExpression;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor};
use std::{
    fs::File,
    io::{BufReader, Read},
};
use tonic::{Code, Status};

pub fn convert_grpc_message_to_query_exp(input: DynamicMessage) -> Result<QueryExpression, Status> {
    let request = input
        .transcode_to::<super::proto_query_models::QueryExpressionRequest>()
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let result_exp = QueryExpression::try_from(request)?;
    Ok(result_exp)
}

pub fn get_method_by_name(
    descriptor: DescriptorPool,
    method_name: String,
) -> Option<MethodDescriptor> {
    let service_lst = descriptor.services().next().unwrap();
    let mut methods = service_lst.methods();
    methods.find(|m| *m.name() == method_name)
}

pub fn get_service_name(descriptor: DescriptorPool) -> Option<String> {
    descriptor
        .services()
        .next()
        .map(|s| s.full_name().to_owned())
}

//https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/DescriptorProtos.FieldDescriptorProto.Type
pub fn get_proto_descriptor(descriptor_dir: String) -> Result<DescriptorPool, GRPCError> {
    let descriptor_set_dir = descriptor_dir;
    let buffer = read_file_as_byte(descriptor_set_dir)?;
    let my_array_byte = buffer.as_slice();
    let pool2 = DescriptorPool::decode(my_array_byte)
        .map_err(|e| GRPCError::ProtoDescriptorError(e.to_string()))?;
    Ok(pool2)
}

pub fn read_file_as_byte(path: String) -> Result<Vec<u8>, GenerationError> {
    let f = File::open(path).map_err(GenerationError::FileCannotOpen)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .map_err(GenerationError::ReadFileBuffer)?;
    Ok(buffer)
}

pub fn create_descriptor_set(
    proto_folder: &str,
    proto_file_name: &str,
) -> Result<String, GenerationError> {
    let proto_file_path = format!("{}/{}", proto_folder.to_owned(), proto_file_name.to_owned());
    let my_path_descriptor = format!("{}/file_descriptor_set.bin", proto_folder.to_owned());
    let mut prost_build_config = prost_build::Config::new();
    prost_build_config.out_dir(proto_folder.to_owned());
    tonic_build::configure()
        .file_descriptor_set_path(&my_path_descriptor)
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .disable_package_emission()
        .build_client(false)
        .build_server(false)
        .out_dir(&proto_folder)
        .compile_with_config(prost_build_config, &[proto_file_path], &[proto_folder])
        .map_err(|e| GenerationError::CannotCreateProtoDescriptor(e.to_string()))?;
    Ok(my_path_descriptor)
}
