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

    // Sample service generated for tests and development
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("generated_films.bin"))
        .compile(&["protos/films.proto"], &["protos"])?;

    // Admin Service & Types
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(
            ".dozer.admin.AppConfig",
            "crate::models::app_config::Config",
        )
        .extern_path(
            ".dozer.admin.ConnectionConfig",
            "crate::models::connection::ConnectionConfig",
        )
        .extern_path(
            ".dozer.admin.Connection",
            "crate::models::connection::Connection",
        )
        .extern_path(
            ".dozer.admin.EthContract",
            "crate::ingestion_types::EthContract",
        )
        .extern_path(
            ".dozer.admin.EthereumFilter",
            "crate::ingestion_types::EthereumFilter",
        )
        .extern_path(
            ".dozer.admin.KafkaConfig",
            "crate::ingestion_types::KafkaConfig",
        )
        .extern_path(
            ".dozer.admin.SnowflakeConfig",
            "crate::ingestion_types::SnowflakeConfig",
        )
        .extern_path(
            ".dozer.admin.GrpcConfig",
            "crate::models::connection::GrpcConfig",
        )
        .extern_path(
            ".dozer.admin.EthereumConfig",
            "crate::ingestion_types::EthConfig",
        )
        .extern_path(
            ".dozer.admin.PostgresConfig",
            "crate::models::connection::PostgresConfig",
        )
        .file_descriptor_set_path(out_dir.join("admin.bin"))
        .compile(&["protos/admin.proto"], &["protos"])
        .unwrap();

    Ok(())
}
