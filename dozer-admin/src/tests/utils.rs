use diesel::r2d2::ConnectionManager;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dozer_orchestrator::cli::generate_connection;
use dozer_types::models::app_config::Config;
use r2d2::{CustomizeConnection, Pool};
use std::error::Error;

use crate::db::{Connection, DB};

#[derive(Debug, Clone)]
pub struct TestConfigId {
    pub app_id: String,
    pub connection_ids: Vec<String>,
    pub source_ids: Vec<String>,
    pub api_ids: Vec<String>,
}
#[derive(Debug)]
struct TestConnectionCustomizer;

impl<E> CustomizeConnection<Connection, E> for TestConnectionCustomizer {
    fn on_acquire(&self, conn: &mut Connection) -> Result<(), E> {
        prepare_test_db(conn);
        Ok(())
    }
    fn on_release(&self, conn: Connection) {
        std::mem::drop(conn);
    }
}
pub type DbPool = Pool<ConnectionManager<Connection>>;
pub fn establish_test_connection(database_url: String) -> DbPool {
    let manager = ConnectionManager::<Connection>::new(database_url);
    r2d2::Pool::builder()
        .max_size(10)
        .connection_customizer(Box::new(TestConnectionCustomizer))
        .build(manager)
        .expect("Failed to create DB pool.")
}

fn prepare_test_db(connection: &mut Connection) {
    run_migrations(connection).unwrap();
}

pub fn database_url_for_test_env() -> String {
    String::from(":memory:")
}

pub fn get_sample_config() -> String {
    let config = Config {
        connections: vec![generate_connection("Postgres")],
        ..Default::default()
    };
    serde_yaml::to_string(&config).unwrap()
}

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

fn run_migrations(
    connection: &mut impl MigrationHarness<DB>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    connection.revert_all_migrations(MIGRATIONS)?;
    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}
