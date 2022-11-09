fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/grpc/services/common/proto/api.proto")?;
    Ok(())
}
