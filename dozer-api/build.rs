use std::{env, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/types.proto"], &["protos"])?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/api.proto"], &["protos"])?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/internal.proto"], &["protos"])?;

    // Sample service generated for tests and development
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("generated_films.bin"))
        .compile(&["protos/films.proto"], &["protos"])?;

    Ok(())
}
