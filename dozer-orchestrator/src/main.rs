use std::sync::Arc;



use dozer_orchestrator::models::api_endpoint::ApiEndpoint;
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::test_connection;
use dozer_orchestrator::{
    models::{
        connection::{Authentication::PostgresAuthentication, Connection, DBType},
        source::{RefreshConfig, Source},
    },
    Orchestrator,
};
use dozer_schema::registry::{_get_client};
use tokio::runtime::Runtime;

fn main() -> anyhow::Result<()> {
    // thread::spawn(|| {
    //     Runtime::new().unwrap().block_on(async {
    //         _serve(None).await.unwrap();
    //     })
    // });
    // thread::sleep(Duration::new(0, 10000));

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
        name: "postgres connection".to_string(),
        id: None,
    };
    test_connection(connection.to_owned()).unwrap();
    let source = Source {
        id: None,
        name: "payment_source".to_string(),
        table_name: "payment".to_string(),
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
