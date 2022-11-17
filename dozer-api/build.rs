fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("protos/api.proto")?;
    tonic_build::compile_protos("protos/internal.proto")?;
    Ok(())
}
