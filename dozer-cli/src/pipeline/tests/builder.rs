use std::sync::Arc;

use crate::pipeline::source_builder::SourceBuilder;
use crate::pipeline::PipelineBuilder;
use dozer_types::ingestion_types::{GrpcConfig, GrpcConfigSchemas};
use dozer_types::models::config::Config;

use dozer_types::indicatif::MultiProgress;
use dozer_types::models::connection::{Connection, ConnectionConfig};
use dozer_types::models::flags::Flags;
use dozer_types::models::source::Source;

fn get_default_config() -> Config {
    let schema_str = include_str!("./schemas.json");
    let grpc_conn = Connection {
        config: Some(ConnectionConfig::Grpc(GrpcConfig {
            schemas: Some(GrpcConfigSchemas::Inline(schema_str.to_string())),
            ..Default::default()
        })),
        name: "grpc_conn".to_string(),
    };

    Config {
        app_name: "multi".to_string(),
        api: Default::default(),
        flags: Default::default(),
        connections: vec![grpc_conn.clone()],
        sources: vec![
            Source {
                name: "grpc_conn_users".to_string(),
                table_name: "users".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                connection: grpc_conn.name.clone(),
                schema: None,
                refresh_config: None,
            },
            Source {
                name: "grpc_conn_customers".to_string(),
                table_name: "customers".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                connection: grpc_conn.name,
                schema: None,
                refresh_config: None,
            },
        ],
        ..Default::default()
    }
}

#[test]
fn load_multi_sources() {
    let config = get_default_config();

    let used_sources = config
        .sources
        .iter()
        .map(|s| s.name.clone())
        .collect::<Vec<_>>();

    let builder = PipelineBuilder::new(
        &config.connections,
        &config.sources,
        config.sql.as_deref(),
        config
            .endpoints
            .into_iter()
            .map(|endpoint| (endpoint, None))
            .collect(),
        MultiProgress::new(),
        Flags::default(),
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);
    let grouped_connections = runtime
        .block_on(builder.get_grouped_tables(&used_sources))
        .unwrap();

    let source_builder = SourceBuilder::new(grouped_connections, None);
    let (_sender, shutdown_receiver) = crate::shutdown::new(&runtime);
    let asm = runtime
        .block_on(source_builder.build_source_manager(&runtime, shutdown_receiver))
        .unwrap();

    asm.get_endpoint(&config.sources[0].name).unwrap();
    asm.get_endpoint(&config.sources[1].name).unwrap();
}
