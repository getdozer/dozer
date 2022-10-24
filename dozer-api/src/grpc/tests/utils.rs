use std::collections::HashMap;

use crate::{
    generator::protoc::{generator::ProtoGenerator, proto_service::GrpcType},
    grpc::util::create_descriptor_set,
    test_utils,
};

pub fn generate_proto(
    dir_path: String,
    schema_name: String,
) -> anyhow::Result<(std::string::String, HashMap<std::string::String, GrpcType>)> {
    let schema: dozer_types::types::Schema = test_utils::get_schema();
    let endpoint = test_utils::get_endpoint();
    let proto_generator = ProtoGenerator::new(schema, schema_name, endpoint)?;
    let generated_proto = proto_generator.generate_proto(dir_path)?;
    Ok(generated_proto)
}

pub fn generate_descriptor(tmp_dir: String, schema_name: String) -> anyhow::Result<String> {
    let descriptor_path = create_descriptor_set(&tmp_dir, &format!("{}.proto", schema_name))?;
    Ok(descriptor_path)
}
