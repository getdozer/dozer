
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .build_client(false)
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .compile(&["protos/api.proto"], &["proto"])
        .unwrap();
    Ok(())
}
