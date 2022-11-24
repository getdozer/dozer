use crate::errors::GRPCError;
use dozer_cache::{cache::expression::QueryExpression, errors::CacheError};
use dozer_types::errors::types::SerializationError;
use dozer_types::serde_json;
use dozer_types::{errors::types::TypeError, serde_json::Value, types::Field};
use prost_reflect::DynamicMessage;
use prost_reflect::{DescriptorPool, MethodDescriptor, SerializeOptions};
use std::{
    fs::File,
    io::{self, BufReader, Read},
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
    service_name: String,
    method_name: String,
) -> Option<MethodDescriptor> {
    for service in descriptor.services() {
        let full_name = service.full_name();
        if full_name == service_name {
            for method in service.methods() {
                if method.name() == method_name {
                    return Some(method);
                }
            }
        }
    }
    None
}

pub fn get_service_name(descriptor: DescriptorPool) -> Vec<String> {
    let mut result: Vec<String> = vec![];
    for service in descriptor.services() {
        let full_name = service.full_name();
        result.push(full_name.to_owned());
    }
    result
}

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

pub fn dozer_field_to_json_value(field: &Field) -> Result<Value, TypeError> {
    match field {
        Field::Int(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::Float(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::Boolean(b) => {
            let result = serde_json::to_value(b).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::String(s) => {
            let result = serde_json::to_value(s).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::UInt(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::Text(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::Binary(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }

        Field::Decimal(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::Timestamp(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::Bson(n) => {
            let result = serde_json::to_value(n).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
        Field::Null => Ok(Value::Null),
        Field::Date(date) => {
            let result = serde_json::to_value(date).map_err(|e| {
                TypeError::SerializationError(SerializationError::Custom(Box::new(e)))
            })?;
            Ok(result)
        }
    }
}

pub fn from_cache_error(error: CacheError) -> Status {
    Status::new(Code::Internal, error.to_string())
}

pub fn convert_grpc_message_to_query_exp(input: DynamicMessage) -> Result<QueryExpression, Status> {
    let json_present = from_dynamic_message_to_json(input)?;
    let mut string_present = json_present.to_string();
    let key_replace = vec![
        "filter", "and", "limit", "skip", "order_by", "eq", "lt", "lte", "gt", "gte",
    ];
    key_replace.iter().for_each(|&key| {
        let from = format!("\"{}\"", key);
        let to = format!("\"${}\"", key);
        string_present = string_present.replace(&from, &to);
    });
    let query_expression: QueryExpression = serde_json::from_str(&string_present)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(query_expression)
}
