use std::{env, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        .extern_path(
            ".dozer.internal.ApplicationDetail",
            "dozer_types::models::app_config::Config",
        )
        .extern_path(
            ".dozer.internal.ApiIndex",
            "dozer_types::models::api_endpoint::ApiIndex",
        )
        .extern_path(
            ".dozer.internal.EndpointInfo",
            "dozer_types::models::api_endpoint::ApiEndpoint",
        )
        .extern_path(
            ".dozer.internal.SourceInfo",
            "dozer_types::models::source::Source",
        )
        .extern_path(
            ".dozer.internal.Authentication",
            "dozer_types::models::connection::AuthenticationWrapper",
        )
        .extern_path(
            ".dozer.internal.ConnectionInfo",
            "dozer_types::models::connection::Connection",
        )
        .extern_path(
            ".dozer.internal.EthereumFilter",
            "dozer_types::ingestion_types::EthereumFilter",
        )
        .extern_path(
            ".dozer.internal.KafkaAuthentication",
            "dozer_types::ingestion_types::KafkaConfig",
        )
        .extern_path(
            ".dozer.internal.SnowflakeAuthentication",
            "dozer_types::ingestion_types::SnowflakeConfig",
        )
        .extern_path(
            ".dozer.internal.EventsAuthentication",
            "dozer_types::models::connection::EventsAuthentication",
        )
        .extern_path(
            ".dozer.internal.EthereumAuthentication",
            "dozer_types::ingestion_types::EthConfig",
        )
        .extern_path(
            ".dozer.internal.PostgresAuthentication",
            "dozer_types::models::connection::PostgresAuthentication",
        )
        .extern_path(
            ".dozer.internal.DBType",
            "dozer_types::models::connection::DBType",
        )
        .extern_path(
            ".dozer.internal.ApiConfig",
            "dozer_types::models::api_config::ApiConfig",
        )
        .extern_path(
            ".dozer.internal.ApiGrpc",
            "dozer_types::models::api_config::ApiGrpc",
        )
        .extern_path(
            ".dozer.internal.ApiRest",
            "dozer_types::models::api_config::ApiRest",
        )
        .extern_path(
            ".dozer.internal.ApiInternal",
            "dozer_types::models::api_config::ApiInternal",
        )
        .compile(&["protos/internal.proto"], &["protos"])?;

    // Sample service generated for tests and development
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("generated_films.bin"))
        .compile(&["protos/films.proto"], &["protos"])?;

    Ok(())
}
