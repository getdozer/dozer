use tonic_build;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/storage.proto")?;
    tonic_build::compile_protos("proto/ingestion.proto")?;

    Ok(())
}
