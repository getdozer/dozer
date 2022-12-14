use crate::db::{persistable::Persistable, pool::establish_connection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dotenvy::dotenv;
use dozer_orchestrator::internal_pipeline_service_client::InternalPipelineServiceClient;
use dozer_types::models::{api_config::ApiInternal, app_config::Config};
use std::{env, error::Error, fs};
use tonic::transport::Channel;

pub async fn init_internal_pipeline_client(
    config: ApiInternal,
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
pub fn reset_db() {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    // check if db file exist
    let db_file_exist = std::path::Path::new(&database_url.to_owned()).exists();
    if db_file_exist {
        fs::remove_file(database_url.to_owned()).unwrap();
    }
    // create new db
    let db_pool = establish_connection(database_url.to_owned());
    let mut db_connection = db_pool.get().unwrap();
    // run migration
    run_migrations(&mut db_connection).unwrap();
}

pub fn init_db_with_config(mut config: Config) -> Config {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db_pool = establish_connection(database_url.to_owned());
    config.save(db_pool).unwrap();
    config
}
