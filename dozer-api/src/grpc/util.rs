use crate::errors::{GRPCError, GenerationError};
use dozer_types::serde_json;
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, SerializeOptions};
use std::{
    fs::File,
    io::{BufReader, Read},
};
use tonic::{Code, Status};
pub fn from_dynamic_message_to_json(input: DynamicMessage) -> Result<serde_json::Value, Status> {
    let mut options = SerializeOptions::new();
    options = options.use_proto_field_name(true);
    let mut serializer = serde_json::Serializer::new(vec![]);
    input
        .serialize_with_options(&mut serializer, &options)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let string_utf8 = String::from_utf8(serializer.into_inner())
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let result: serde_json::Value = serde_json::from_str(&string_utf8)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(result)
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
        .build_client(false)
        .build_server(false)
        .out_dir(proto_folder)
        .compile_with_config(prost_build_config, &[proto_file_path], &[proto_folder])
        .map_err(|e| GenerationError::CannotCreateProtoDescriptor(e.to_string()))?;
    Ok(my_path_descriptor)
}
