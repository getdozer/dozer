use dozer_cache::cache::expression::{FilterExpression, QueryExpression, SortOptions};
use dozer_types::serde_json::{self, json, Value};
use prost_reflect::{DescriptorPool, DynamicMessage, MethodDescriptor, SerializeOptions};
use std::{
    fs::File,
    io::{BufReader, Read},
};
use tonic::{Code, Status};

pub fn from_dynamic_message_to_json(input: DynamicMessage) -> Result<Value, Status> {
    let mut options = SerializeOptions::new();
    options = options.use_proto_field_name(true);
    let mut serializer = serde_json::Serializer::new(vec![]);
    input
        .serialize_with_options(&mut serializer, &options)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let string_utf8 = String::from_utf8(serializer.into_inner())
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let result: Value = serde_json::from_str(&string_utf8)
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    Ok(result)
}

pub fn convert_grpc_message_to_query_exp(input: DynamicMessage) -> Result<QueryExpression, Status> {
    let json_present = from_dynamic_message_to_json(input)?;
    let default_filter = json!("");
    let filter_field = json_present.get("filter").unwrap_or(&default_filter);

    let default_order_by = json!([]);
    let order_by_field = json_present.get("order_by").unwrap_or(&default_order_by);

    let default_skip = json!(0);
    let skip_field = json_present.get("skip").unwrap_or(&default_skip);

    let default_limit = json!(50);
    let limit_field = json_present.get("limit").unwrap_or(&default_limit);

    let filter_exp_str = filter_field.as_str().unwrap_or_default();
    let filter_exp = serde_json::from_str::<FilterExpression>(filter_exp_str)
        .map_or_else(|_| Option::None, Option::Some);

    let order_by: Vec<SortOptions> = serde_json::from_value(order_by_field.to_owned())
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let limit: usize = serde_json::from_value(limit_field.to_owned())
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let skip: usize = serde_json::from_value(skip_field.to_owned())
        .map_err(|err| Status::new(Code::Internal, err.to_string()))?;
    let result_exp = QueryExpression::new(filter_exp, order_by, limit, skip);
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
pub fn get_proto_descriptor(descriptor_dir: String) -> anyhow::Result<DescriptorPool> {
    let descriptor_set_dir = descriptor_dir;
    let buffer = read_file_as_byte(descriptor_set_dir)?;
    let my_array_byte = buffer.as_slice();
    let pool2 = DescriptorPool::decode(my_array_byte)?;
    Ok(pool2)
}

pub fn read_file_as_byte(path: String) -> anyhow::Result<Vec<u8>> {
    let f = File::open(path)?;
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;
    Ok(buffer)
}

pub fn create_descriptor_set(proto_path: &str) -> anyhow::Result<String> {
    let my_path = "proto_build".to_owned();
    let my_path_descriptor = "proto_build/file_descriptor_set.bin".to_owned();
    let mut prost_build_config = prost_build::Config::new();
    prost_build_config.out_dir(my_path.to_owned());
    let mut prost_build_config2 = prost_build::Config::new();
    prost_build_config2.out_dir(my_path.to_owned());
    tonic_build::configure()
        .file_descriptor_set_path(&my_path_descriptor)
        .disable_package_emission()
        .build_client(false)
        .build_server(false)
        .out_dir(&my_path)
        .compile_with_config(prost_build_config2, &[proto_path], &["proto_build/"])?;
    Ok(my_path_descriptor)
}
