use std::{env, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("protos/types.proto")?;
    tonic_build::compile_protos("protos/api.proto")?;
    tonic_build::compile_protos("protos/internal.proto")?;

    // Sample service generated for tests and development
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("generated_films.bin"))
        .compile(&["protos/films.proto"], &["protos"])?;

    Ok(())
}
