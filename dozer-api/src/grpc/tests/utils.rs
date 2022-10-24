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
    let proto_generator = ProtoGenerator::new(schema, schema_name.to_owned(), endpoint)?;
    let generated_proto = proto_generator.generate_proto(dir_path.to_owned())?;
    Ok(generated_proto)
}

pub fn generate_descriptor(tmp_dir: String, schema_name: String) -> anyhow::Result<String> {
    let schema_name = String::from("film");
    let descriptor_path = create_descriptor_set(
        &tmp_dir.to_owned(),
        &format!("{}.proto", schema_name.to_owned()),
    )?;
    Ok(descriptor_path)
}
