use tonic_build;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute(
            "dozer_api_grpc.PostgresAuthentication",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile(&["protos/api.proto"], &["proto"])
        .unwrap();
    Ok(())
}
