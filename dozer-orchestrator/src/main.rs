use std::sync::Arc;

use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::test_connection;
use dozer_orchestrator::Orchestrator;
use dozer_schema::registry::_get_client;
use dozer_types::models::{
    api_endpoint::ApiEndpoint,
    connection::{Authentication::PostgresAuthentication, Connection, DBType},
    source::{RefreshConfig, Source},
};
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    film_test()
}

fn film_test() -> anyhow::Result<()> {
    let client = Runtime::new()
        .unwrap()
        .block_on(async { _get_client().await.unwrap() });

    let connection: Connection = Connection {
        db_type: DBType::Postgres,
        authentication: PostgresAuthentication {
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            database: "large_film".to_string(),
        },
        name: "film".to_string(),
        id: None,
    };
    test_connection(connection.to_owned()).unwrap();
    let source = Source {
        id: None,
        name: "film_source".to_string(),
        table_name: "film".to_string(),
        connection,
        history_type: None,
        refresh_config: RefreshConfig::RealTime,
    };
    let mut dozer = Dozer::new(Arc::new(client));
    let mut sources = Vec::new();
    sources.push(source);
    dozer.add_sources(sources);
    dozer.add_endpoint(ApiEndpoint {
        id: None,
        name: "film_api".to_string(),
        path: "/films".to_string(),
        enable_rest: false,
        enable_grpc: true,
        sql: "select description from film where 1=1;".to_string(),
    });
    dozer.run()?;
    Ok(())
}

fn actor_test() -> anyhow::Result<()> {
    let client = Runtime::new()
        .unwrap()
        .block_on(async { _get_client().await.unwrap() });

    let connection: Connection = Connection {
        db_type: DBType::Postgres,
        authentication: PostgresAuthentication {
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            database: "pagila".to_string(),
        },
        name: "actor".to_string(),
        id: None,
    };
    test_connection(connection.to_owned()).unwrap();
    let source = Source {
        id: None,
        name: "actor_source".to_string(),
        table_name: "actor".to_string(),
        connection,
        history_type: None,
        refresh_config: RefreshConfig::RealTime,
    };
    let mut dozer = Dozer::new(Arc::new(client));
    let mut sources = Vec::new();
    sources.push(source);
    dozer.add_sources(sources);
    dozer.add_endpoint(ApiEndpoint {
        id: None,
        name: "actor_api".to_string(),
        path: "/actors".to_string(),
        enable_rest: false,
        enable_grpc: true,
        sql: "select actor_id from actor where 1=1;".to_string(),
    });
    dozer.run()?;
    Ok(())
}
