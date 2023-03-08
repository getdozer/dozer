use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(
            ".dozer_admin_grpc.AppConfig",
            "dozer_types::models::app_config::Config",
        )
        .extern_path(
            ".dozer_admin_grpc.ConnectionConfig",
            "dozer_types::models::connection::ConnectionConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.Connection",
            "dozer_types::models::connection::Connection",
        )
        .extern_path(
            ".dozer_admin_grpc.EthContract",
            "dozer_types::ingestion_types::EthContract",
        )
        .extern_path(
            ".dozer_admin_grpc.EthereumFilter",
            "dozer_types::ingestion_types::EthereumFilter",
        )
        .extern_path(
            ".dozer_admin_grpc.KafkaConfig",
            "dozer_types::ingestion_types::KafkaConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.SnowflakeConfig",
            "dozer_types::ingestion_types::SnowflakeConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.GrpcConfig",
            "dozer_types::models::connection::GrpcConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.EthereumConfig",
            "dozer_types::ingestion_types::EthConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.PostgresConfig",
            "dozer_types::models::connection::PostgresConfig",
        )
        .build_client(false)
        .file_descriptor_set_path(out_dir.join("dozer_admin_grpc_descriptor.bin"))
        .compile(&["protos/admin.proto"], &["protos"])
        .unwrap();

    Ok(())
}
