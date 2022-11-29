use crate::db::connection::DbConnection;
use diesel::{r2d2::ConnectionManager, RunQueryDsl, SqliteConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dozer_types::models::connection::DBType;
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
    let manager = ConnectionManager::<SqliteConnection>::new(&database_url);
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
    TestConfigId {
        app_id: "a04376da-3af3-4051-a725-ed0073b3b598".to_owned(),
        connection_ids: vec!["dcd4fc24-d0d8-4f5f-9ec2-dfa74ddbc353".to_owned()],
        source_ids: vec!["eb83ae6d-3bc8-4d10-a804-541f60f551ea".to_owned()],
        api_ids: vec!["de3052fc-affb-46f8-b8c1-0ac69ee91a4f".to_owned()],
    }
}
pub fn database_url_for_test_env() -> String {
    String::from(":memory:")
}

fn fake_dbconnection(db_type: DBType) -> DbConnection {
    match db_type {
        DBType::Postgres => DbConnection {
            auth: r#"{"authentication":{"Postgres":{"database":"users","user":"postgres","host":"localhost","port":5432,"password":"postgres"}}}"#.to_owned(),
            name: "postgres_connection".to_owned(),
            db_type: "postgres".to_owned(),
            ..Default::default()
        },
        DBType::Ethereum => DbConnection {
            auth: r#"{"authentication":{"Postgres":{"database":"users","user":"postgres","host":"localhost","port":5432,"password":"postgres"}}}"#.to_owned(),
            name: "eth_connection".to_owned(),
            db_type: "eth".to_owned(),
            ..Default::default()
        },
        DBType::Events => DbConnection {
            auth: r#"{"authentication":{"Events":{"database":"users"}}}"#.to_owned(),
            name: "events_connection".to_owned(),
            db_type: "events".to_owned(),
            ..Default::default()
        },
        DBType::Snowflake => DbConnection {
            auth: r#"{"authentication":{"Snowflake":{"server":"tx06321.eu-north-1.aws.snowflakecomputing.com","port":"443","user":"karolisgud","password":"uQ@8S4856G9SHP6","database":"DOZER_SNOWFLAKE_SAMPLE_DATA","schema":"PUBLIC","warehouse":"TEST","driver":"{/opt/snowflake/snowflakeodbc/lib/universal/libSnowflake.dylib}"}}}"#.to_owned(),
            name: "snowflake_connection".to_owned(),
            db_type: "snowflake".to_owned(),
            ..Default::default()
        },
        DBType::Kafka => DbConnection {
            auth: r#"{"authentication":{"Kafka":{"broker":"localhost:9092","topic":"dbserver1.public.products"}}}"#.to_owned(),
            name: "kafka_debezium_connection".to_owned(),
            db_type: "kafka".to_owned(),
            ..Default::default()
        }
    }
}

fn setup_data(connection: &mut SqliteConnection, config_id: TestConfigId) {
    // let generated_app_id = uuid::Uuid::new_v4().to_string();
    // create app
    insert_apps(connection, config_id.app_id.to_owned());

    //let generated_postgres_connection_id = uuid::Uuid::new_v4().to_string();
    let fake_postgres: DbConnection = fake_dbconnection(DBType::Postgres);
    insert_connections(
        connection,
        config_id.connection_ids[0].to_owned(),
        config_id.app_id.to_owned(),
        fake_postgres.db_type,
        fake_postgres.auth,
        fake_postgres.name,
    );

    //let generated_source_id = uuid::Uuid::new_v4().to_string();
    insert_sources(
        connection,
        config_id.source_ids[0].to_owned(),
        config_id.app_id.to_owned(),
        "source_name".to_owned(),
        "users".to_owned(),
        config_id.connection_ids[0].to_owned(),
    );

    //let generated_endpoint_id = uuid::Uuid::new_v4().to_string();
    insert_endpoints(
        connection,
        config_id.api_ids[0].to_owned(),
        config_id.app_id.to_owned(),
        "users".to_owned(),
        "/users".to_owned(),
        "select id, email, phone from users where 1=1;".to_owned(),
        "id".to_owned(),
    );
}

fn insert_apps(connection: &mut SqliteConnection, app_id: String) {
    diesel::sql_query(format!("INSERT INTO apps (id, name, created_at, updated_at) VALUES('{}', \'app_name\', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);", app_id))
        .execute(connection)
        .unwrap();
}

fn insert_connections(
    connection: &mut SqliteConnection,
    connection_id: String,
    app_id: String,
    db_type: String,
    auth: String,
    name: String,
) {
    diesel::sql_query(
        format!("INSERT INTO connections (id, app_id, auth, name, db_type, created_at, updated_at) VALUES('{}', '{}', '{}', '{}', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);",
        connection_id, app_id, auth, name, db_type))
    .execute(connection)
    .unwrap();
}

fn insert_sources(
    connection: &mut SqliteConnection,
    source_id: String,
    app_id: String,
    name: String,
    table_name: String,
    connection_id: String,
) {
    diesel::sql_query(
        format!("INSERT INTO sources (id, app_id, name, table_name, connection_id, created_at, updated_at) VALUES('{}', '{}', '{}', '{}', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);",
        source_id, app_id, name,table_name, connection_id))
    .execute(connection)
    .unwrap();
}

fn insert_endpoints(
    connection: &mut SqliteConnection,
    endpoint_id: String,
    app_id: String,
    name: String,
    path: String,
    sql: String,
    primary_keys: String,
) {
    diesel::sql_query(
        format!("INSERT INTO endpoints (id, app_id, name, \"path\", enable_rest, enable_grpc, \"sql\", primary_keys, created_at, updated_at) VALUES( '{}', '{}', '{}', '{}', 1, 1, '{}','{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);"
        ,endpoint_id, app_id, name,path, sql, primary_keys))
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
