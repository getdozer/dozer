use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let _manifest_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .extern_path(
            ".dozer_admin_grpc.ApplicationDetail",
            "dozer_types::models::app_config::Config",
        )
        .extern_path(
            ".dozer_admin_grpc.ApiIndex",
            "dozer_types::models::api_endpoint::ApiIndex",
        )
        .extern_path(
            ".dozer_admin_grpc.EndpointInfo",
            "dozer_types::models::api_endpoint::ApiEndpoint",
        )
        .extern_path(
            ".dozer_admin_grpc.SourceInfo",
            "dozer_types::models::source::Source",
        )
        .extern_path(
            ".dozer_admin_grpc.Authentication",
            "dozer_types::models::connection::AuthenticationWrapper",
        )
        .extern_path(
            ".dozer_admin_grpc.ConnectionInfo",
            "dozer_types::models::connection::Connection",
        )
        .extern_path(
            ".dozer_admin_grpc.EthereumFilter",
            "dozer_types::ingestion_types::EthereumFilter",
        )
        .extern_path(
            ".dozer_admin_grpc.KafkaAuthentication",
            "dozer_types::ingestion_types::KafkaConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.SnowflakeAuthentication",
            "dozer_types::ingestion_types::SnowflakeConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.EventsAuthentication",
            "dozer_types::models::connection::EventsAuthentication",
        )
        .extern_path(
            ".dozer_admin_grpc.EthereumAuthentication",
            "dozer_types::ingestion_types::EthConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.PostgresAuthentication",
            "dozer_types::models::connection::PostgresAuthentication",
        )
        .extern_path(
            ".dozer_admin_grpc.DBType",
            "dozer_types::models::connection::DBType",
        )
        .extern_path(
            ".dozer_admin_grpc.ApiConfig",
            "dozer_types::models::api_config::ApiConfig",
        )
        .extern_path(
            ".dozer_admin_grpc.ApiGrpc",
            "dozer_types::models::api_config::ApiGrpc",
        )
        .extern_path(
            ".dozer_admin_grpc.ApiRest",
            "dozer_types::models::api_config::ApiRest",
        )
        .extern_path(
            ".dozer_admin_grpc.ApiInternal",
            "dozer_types::models::api_config::ApiInternal",
        )
        .build_client(false)
        .file_descriptor_set_path(out_dir.join("dozer_admin_grpc_descriptor.bin"))
        .compile(&["protos/api.proto"], &["proto"])
        .unwrap();

    let profile = std::env::var("PROFILE").unwrap();
    let mut manifest_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    manifest_path.pop();
    manifest_path.push(format!("target/{:}/dozer", profile));
    let dozer_bin_path = manifest_path;
    if !Path::exists(&dozer_bin_path) {
        let mut orchestrator_cli = Command::new("cargo");
        let params = vec!["build", "-p", "dozer-orchestrator", "--bin", "dozer"];
        orchestrator_cli.args(params);
        // build release if current build profile is release
        if profile == "release" {
            orchestrator_cli.arg("-r");
        }
        let execute_result = orchestrator_cli.status().unwrap();
        if !execute_result.success() {
            panic!("Cannot build dozer-orchestrator cli");
        }
    }
    let _ = Command::new("cp").args([dozer_bin_path, out_dir]).status();
    Ok(())
}
