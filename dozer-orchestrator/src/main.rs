use dozer_ingestion::connectors::{postgres::connector::PostgresConfig, storage::RocksConfig};
use dozer_orchestrator::orchestration::{
    builder::{ Dozer},
    db::service::DbPersistentService,
    models::{
        connection::{Authentication::PostgresAuthentication, Connection, DBType},
        source::{HistoryType, MasterHistoryConfig, RefreshConfig, Source},
    },
};
fn main() {
    let db_url = "dozer.db";
    let persistent_service: DbPersistentService = DbPersistentService::new(db_url.to_owned());

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
    Dozer::test_connection(connection.to_owned()).unwrap();

    let connection_id = persistent_service
        .save_connection(connection.clone())
        .unwrap();
    let connection = persistent_service.read_connection(connection_id).unwrap();
    let source = Source {
        id: None,
        name: "source name".to_string(),
        dest_table_name: "SOURCE_NAME".to_string(),
        connection,
        history_type: HistoryType::Master(MasterHistoryConfig::AppendOnly {
            unique_key_field: "id".to_string(),
            open_date_field: "created_date".to_string(),
            closed_date_field: "updated_date".to_string(),
        }),
        refresh_config: RefreshConfig::RealTime,
    };
    let mut dozer = Dozer::new();
    let mut sources = Vec::new();
    sources.push(source);
    dozer.add_sources(sources);
    dozer.run();
}
