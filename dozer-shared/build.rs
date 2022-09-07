use tonic_build;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/storage.proto")?;
    tonic_build::configure()
    .type_attribute("ingestion.TableInfo", "#[derive(serde::Deserialize)]")
    .type_attribute("ingestion.ColumnInfo", "#[derive(serde::Serialize, serde::Deserialize)]")
    .compile(&["proto/ingestion.proto"], &["proto"])
    .unwrap();
    Ok(())
}
