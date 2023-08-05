use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("ingest.bin"))
        .compile(&["protos/ingest.proto"], &["protos"])?;

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/types.proto"], &["protos"])?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/common.proto"], &["protos"])?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/health.proto"], &["protos"])?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/internal.proto"], &["protos"])?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/auth.proto"], &["protos"])?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["protos/live.proto"], &["protos"])?;

    // Sample service generated for tests and development
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("generated_films.bin"))
        .compile(&["protos/films.proto"], &["protos"])?;

    // Cloud Service & Types
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(
            ".dozer.cloud.Endpoint",
            "crate::models::api_endpoint::ApiEndpoint",
        )
        .extern_path(".dozer.cloud.Source", "crate::models::source::Source")
        .extern_path(".dozer.cloud.AppConfig", "crate::models::config::Config")
        .extern_path(
            ".dozer.cloud.ConnectionConfig",
            "crate::models::connection::ConnectionConfig",
        )
        .extern_path(
            ".dozer.cloud.Connection",
            "crate::models::connection::Connection",
        )
        .extern_path(
            ".dozer.cloud.EthContract",
            "crate::ingestion_types::EthContract",
        )
        .extern_path(
            ".dozer.cloud.EthereumFilter",
            "crate::ingestion_types::EthereumFilter",
        )
        .extern_path(
            ".dozer.cloud.DeltaLakeConfig",
            "crate::ingestion_types::DeltaLakeConfig",
        )
        .extern_path(
            ".dozer.cloud.LocalStorage",
            "crate::ingestion_types::LocalStorage",
        )
        .extern_path(
            ".dozer.cloud.S3Storage",
            "crate::ingestion_types::S3Storage",
        )
        .extern_path(
            ".dozer.cloud.KafkaConfig",
            "crate::ingestion_types::KafkaConfig",
        )
        .extern_path(
            ".dozer.cloud.SnowflakeConfig",
            "crate::ingestion_types::SnowflakeConfig",
        )
        .extern_path(
            ".dozer.cloud.GrpcConfig",
            "crate::models::connection::GrpcConfig",
        )
        .extern_path(
            ".dozer.cloud.EthereumConfig",
            "crate::ingestion_types::EthConfig",
        )
        .extern_path(
            ".dozer.cloud.PostgresConfig",
            "crate::models::connection::PostgresConfig",
        )
        .file_descriptor_set_path(out_dir.join("cloud.bin"))
        .compile(&["protos/cloud.proto"], &["protos"])
        .unwrap();

    Ok(())
}
