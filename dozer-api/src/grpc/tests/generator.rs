use crate::grpc::tests::utils::{generate_descriptor, generate_proto};
use std::path::Path;
use tempdir::TempDir;

#[test]
fn test_generate_proto() -> anyhow::Result<()> {
    let tmp_dir = TempDir::new("proto_generated")?;
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let schema_name = String::from("film");
    let proto_result = generate_proto(tmp_dir_path, schema_name.to_owned())?;
    let tempdir_path = String::from(tmp_dir.path().to_str().unwrap());
    let path_proto_generated =
        Path::new(&format!("{}/{}.proto", tempdir_path, schema_name)).exists();
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
    generate_proto(tmp_dir_path.to_owned(), schema_name.to_owned())?;
    let path_to_descriptor = generate_descriptor(tmp_dir_path, schema_name)?;
    let check_exist = Path::new(&path_to_descriptor).exists();
    assert!(check_exist, "protofile must be existed !");
    Ok(())
}
