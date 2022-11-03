use crate::{
    errors::GenerationError,
    grpc::tests::utils::{generate_descriptor, generate_proto},
    test_utils,
};
use std::path::Path;
use tempdir::TempDir;

#[test]
fn test_generate_proto() -> Result<(), GenerationError> {
    let tmp_dir = TempDir::new("proto_generated").map_err(GenerationError::TmpFile)?;
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let schema_name = String::from("film");
    let schema = test_utils::get_schema();
    let proto_result = generate_proto(tmp_dir_path, schema_name.to_owned(), schema)?;
    let tempdir_path = String::from(tmp_dir.path().to_str().unwrap());
    let path_proto_generated =
        Path::new(&format!("{}/{}.proto", tempdir_path, schema_name)).exists();
    assert_eq!(
        proto_result.1.len(),
        7,
        " 7 service message must be generated"
    );
    assert!(path_proto_generated, "protofile must be existed !");
    Ok(())
}
#[test]
fn test_generate_proto_with_time_stamp() -> Result<(), GenerationError> {
    let tmp_dir = TempDir::new("proto_generated").map_err(GenerationError::TmpFile)?;
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let schema_name = String::from("film");
    let schema = test_utils::get_schema_with_timestamp();
    let proto_result = generate_proto(tmp_dir_path, schema_name.to_owned(), schema)?;
    let tempdir_path = String::from(tmp_dir.path().to_str().unwrap());
    let path_proto_generated =
        Path::new(&format!("{}/{}.proto", tempdir_path, schema_name)).exists();
    assert_eq!(
        proto_result.1.len(),
        7,
        " 7 service message must be generated"
    );
    assert!(path_proto_generated, "protofile must be existed !");
    Ok(())
}
#[test]
fn test_generate_descriptor() -> Result<(), GenerationError> {
    let tmp_dir = TempDir::new("proto_generated").map_err(GenerationError::TmpFile)?;
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    //let tmp_dir_path = "proto_generated".to_string();
    let schema_name = String::from("film");
    let schema = test_utils::get_schema();
    generate_proto(tmp_dir_path.to_owned(), schema_name.to_owned(), schema)?;
    let path_to_descriptor = generate_descriptor(tmp_dir_path, schema_name)?;
    let check_exist = Path::new(&path_to_descriptor).exists();
    assert!(check_exist, "proto file must be present!");
    Ok(())
}
