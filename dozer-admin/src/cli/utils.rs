use crate::db::{persistable::Persistable, pool::establish_connection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dotenvy::dotenv;
use dozer_orchestrator::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_types::models::{api_config::ApiPipelineInternal, app_config::Config};
use std::{env, error::Error, fs, process::Command};
use tonic::transport::Channel;

pub async fn init_internal_pipeline_client(
    config: ApiPipelineInternal,
) -> Result<InternalPipelineServiceClient<Channel>, Box<dyn std::error::Error>> {
    let address = format!("http://{:}:{:}", config.host, config.port);
    let client = dozer_orchestrator::internal_pipeline_service_client::InternalPipelineServiceClient::connect(address).await?;
    Ok(client)
}

type DB = diesel::sqlite::Sqlite;
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
fn run_migrations(
    connection: &mut impl MigrationHarness<DB>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    connection.revert_all_migrations(MIGRATIONS)?;
    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}
pub fn get_db_path() -> String {
    env::var("DATABASE_URL").unwrap_or("dozer.db".to_owned())
}
pub fn reset_db() {
    dotenv().ok();
    let database_url = get_db_path();
    // check if db file exist
    let db_file_exist = std::path::Path::new(&database_url).exists();
    if db_file_exist {
        fs::remove_file(&database_url).unwrap();
    }
    // create new db
    let db_pool = establish_connection(database_url);
    let mut db_connection = db_pool.get().unwrap();
    // run migration
    run_migrations(&mut db_connection).unwrap();
}

pub fn init_db_with_config(mut config: Config) -> Config {
    let database_url = get_db_path();
    let db_pool = establish_connection(database_url);
    config.save(db_pool).unwrap();
    config
}

pub fn kill_process_at(port: u16) {
    let mut check_ports_used = Command::new("lsof");
    check_ports_used.args(["-t", &format!("-i:{:}", port)]);
    let check_port_result = check_ports_used
        .output()
        .expect("failed to execute process");
    let check_port_result_str = String::from_utf8(check_port_result.stdout).unwrap();
    if !check_port_result_str.is_empty() {
        let ports: Vec<String> = check_port_result_str
            .split('\n')
            .into_iter()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let _clear_grpc_port_command = Command::new("kill")
            .args(["-9", &ports[ports.len() - 1]])
            .output()
            .unwrap();
    }
}
