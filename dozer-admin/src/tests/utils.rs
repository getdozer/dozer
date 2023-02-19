use diesel::{r2d2::ConnectionManager, RunQueryDsl, SqliteConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dozer_orchestrator::cli::generate_connection;
use dozer_types::models::app_config::Config;
use r2d2::{CustomizeConnection, Pool};
use std::error::Error;
type DB = diesel::sqlite::Sqlite;

#[derive(Debug, Clone)]
pub struct TestConfigId {
    pub app_id: String,
    pub connection_ids: Vec<String>,
    pub source_ids: Vec<String>,
    pub api_ids: Vec<String>,
}
#[derive(Debug)]
struct TestConnectionCustomizer;

impl<E> CustomizeConnection<SqliteConnection, E> for TestConnectionCustomizer {
    fn on_acquire(&self, conn: &mut SqliteConnection) -> Result<(), E> {
        let setup_ids = get_setup_ids();
        prepare_test_db(conn, setup_ids);
        Ok(())
    }
    fn on_release(&self, conn: SqliteConnection) {
        std::mem::drop(conn);
    }
}
pub type DbPool = Pool<ConnectionManager<SqliteConnection>>;
pub fn establish_test_connection(database_url: String) -> DbPool {
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    r2d2::Pool::builder()
        .max_size(10)
        .connection_customizer(Box::new(TestConnectionCustomizer))
        .build(manager)
        .expect("Failed to create DB pool.")
}

fn prepare_test_db(connection: &mut SqliteConnection, config_id: TestConfigId) {
    run_migrations(connection).unwrap();
    setup_data(connection, config_id)
}

pub fn get_setup_ids() -> TestConfigId {
    let connection_ids: Vec<String> = vec![
        "9cd38b34-3100-4b61-99fb-ca3626b90f59".to_owned(),
        "2afd6d1f-f739-4f02-9683-b469011936a4".to_owned(),
        "dc5d0a89-7b7a-4ab1-88a0-f23ec5c73482".to_owned(),
        "67df73b7-a322-4ff7-86b4-d7a5b12416d9".to_owned(),
        "7a82ead6-bfd2-4336-805c-a7058dfac3a6".to_owned(),
    ];

    let source_ids: Vec<String> = vec![
        "ebec89f4-80c7-4519-99d3-94cf55669c2b".to_owned(),
        "0ea2cb76-1103-476d-935c-fe5f745bad53".to_owned(),
        "28732cb6-7a68-4e34-99f4-99e356daa06d".to_owned(),
        "bce87d76-93dc-42af-bffa-d47743f4c7fa".to_owned(),
        "d0356a18-77f5-479f-a690-536d086707d8".to_owned(),
    ];
    TestConfigId {
        app_id: "a04376da-3af3-4051-a725-ed0073b3b598".to_owned(),
        connection_ids,
        source_ids,
        api_ids: vec!["de3052fc-affb-46f8-b8c1-0ac69ee91a4f".to_owned()],
    }
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

fn setup_data(connection: &mut SqliteConnection, config_id: TestConfigId) {
    // let generated_app_id = uuid::Uuid::new_v4().to_string();
    // create app
    insert_apps(connection, config_id.app_id.to_owned(), get_sample_config());
}

fn insert_apps(connection: &mut SqliteConnection, app_id: String, config: String) {
    diesel::sql_query(format!("INSERT INTO apps (id, name, config, created_at, updated_at) VALUES('{app_id}', \'app_name\', '{config}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);"))
        .execute(connection)
        .unwrap();
}
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

fn run_migrations(
    connection: &mut impl MigrationHarness<DB>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    connection.revert_all_migrations(MIGRATIONS)?;
    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}
