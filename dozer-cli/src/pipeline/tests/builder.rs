use std::sync::Arc;

use crate::pipeline::source_builder::SourceBuilder;
use crate::pipeline::PipelineBuilder;
use dozer_core::shutdown;
use dozer_types::models::config::Config;
use dozer_types::models::ingestion_types::{ConfigSchemas, GrpcConfig};

use dozer_types::models::connection::{Connection, ConnectionConfig};
use dozer_types::models::flags::Flags;
use dozer_types::models::source::Source;

fn get_default_config() -> Config {
    let schema_str = include_str!("./schemas.json");
    let grpc_conn = Connection {
        config: ConnectionConfig::Grpc(GrpcConfig {
            host: None,
            port: None,
            adapter: None,
            schemas: ConfigSchemas::Inline(schema_str.to_string()),
        }),
        name: "grpc_conn".to_string(),
    };

    Config {
        app_name: "multi".to_string(),
        version: 1,
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
                refresh_config: Default::default(),
            },
            Source {
                name: "grpc_conn_customers".to_string(),
                table_name: "customers".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                connection: grpc_conn.name,
                schema: None,
                refresh_config: Default::default(),
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
        &config.sinks,
        Default::default(),
        Flags::default(),
        &config.udfs,
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);
    let grouped_connections = runtime
        .block_on(builder.get_grouped_tables(&runtime, &used_sources))
        .unwrap();

    let source_builder = SourceBuilder::new(grouped_connections, Default::default());
    let (_sender, shutdown_receiver) = shutdown::new(&runtime);
    let asm = runtime
        .block_on(source_builder.build_source_manager(&runtime, shutdown_receiver))
        .unwrap();

    asm.get_endpoint(&config.sources[0].name).unwrap();
    asm.get_endpoint(&config.sources[1].name).unwrap();
}

#[test]
fn invalid_unused_udf_fails_before_connector_setup_with_or_without_sql() {
    use dozer_types::models::udf_config::{UdfConfig, UdfType, WasmConfig};
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
    let dir = tempfile::tempdir().unwrap();
    for sql in [
        None,
        Some("SELECT id INTO output FROM grpc_conn_users".into()),
    ] {
        let mut config = get_default_config();
        config.sql = sql;
        // A source referencing a missing connection would fail discovery if reached.
        config.connections.clear();
        config.udfs = vec![UdfConfig {
            name: "unused_function".into(),
            config: UdfType::Wasm(WasmConfig {
                path: dir.path().join("missing.wasm").to_str().unwrap().into(),
            }),
        }];
        let builder = PipelineBuilder::new(
            &config.connections,
            &config.sources,
            config.sql.as_deref(),
            &config.sinks,
            Default::default(),
            Flags::default(),
            &config.udfs,
        );
        let (_sender, receiver) = shutdown::new(&runtime);
        let error = runtime
            .block_on(builder.build(&runtime, receiver))
            .err()
            .unwrap()
            .to_string();
        #[cfg(feature = "wasm")]
        assert!(
            error.contains("cannot initialize WASM UDF unused_function"),
            "{error}"
        );
        #[cfg(not(feature = "wasm"))]
        assert!(error.contains("requires the wasm Cargo feature"), "{error}");
    }
}
