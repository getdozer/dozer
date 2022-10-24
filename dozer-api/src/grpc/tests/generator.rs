use crate::generator::protoc::proto_service::GrpcType;
use crate::grpc::util::create_descriptor_set;
use crate::{generator::protoc::generator::ProtoGenerator, test_utils};
use std::collections::HashMap;
use std::path::Path;
use tempdir::TempDir;

fn generate_proto(
    dir_path: String,
    schema_name: String,
) -> anyhow::Result<(
    std::string::String, HashMap<std::string::String, GrpcType>
)> {
    let schema: dozer_types::types::Schema = test_utils::get_schema();
    let endpoint = test_utils::get_endpoint();
    let proto_generator = ProtoGenerator::new(schema, schema_name.to_owned(), endpoint)?;
    let generated_proto = proto_generator.generate_proto(dir_path.to_owned())?;
    Ok(generated_proto)
}

fn generate_descriptor(tmp_dir: String, schema_name: String) -> anyhow::Result<String> {
    generate_proto(tmp_dir.to_owned(), schema_name)?;
    let schema_name = String::from("film");
    let descriptor_path =
        create_descriptor_set(&tmp_dir.to_owned(), &format!("{}.proto", schema_name.to_owned()))?;
    Ok(descriptor_path)
}

#[test]
fn test_generate_proto() -> anyhow::Result<()> {
    let tmp_dir = TempDir::new("proto_generated")?;
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());

    let schema_name = String::from("film");
    let proto_result = generate_proto(tmp_dir_path.to_owned(), schema_name.to_owned())?;

    let tempdir_path = String::from(tmp_dir.path().to_str().unwrap());
    let path_proto_generated = Path::new(&format!(
        "{}/{}.proto",
        tempdir_path.to_owned(),
        schema_name.to_owned()
    ))
    .exists();
    assert_eq!(
        proto_result.1.len(),
        3,
        " 3 service message must be generated"
    );
    assert!(path_proto_generated, "protofile must be existed !");
    Ok(())
}

#[test]
fn test_generate_descriptor() -> anyhow::Result<()> {
    let tmp_dir = TempDir::new("proto_generated")?;
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let schema_name = String::from("film");
    let path_to_descriptor = generate_descriptor(tmp_dir_path, schema_name)?;
    let check_exist = Path::new(&path_to_descriptor).exists();
    assert!(check_exist, "protofile must be existed !");
    Ok(())
}
