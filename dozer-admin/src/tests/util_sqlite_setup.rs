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
fn get_db_type_supported() -> Vec<DBType> {
    vec![
        DBType::Postgres,
        DBType::Ethereum,
        DBType::Events,
        DBType::Snowflake,
        DBType::Kafka,
    ]
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

fn fake_dbconnection(db_type: DBType) -> DbConnection {
    match db_type {
        DBType::Postgres => DbConnection {
            auth: r#"{"Postgres":{"user":"users","password":"postgres","host":"localhost","port":5432,"database":"postgres"}}"#.to_owned(),
            name: "postgres_connection".to_owned(),
            db_type: "postgres".to_owned(),
            ..Default::default()
        },
        DBType::Ethereum => DbConnection {
            auth: r#"{"Ethereum":{"filter":{"from_block":0,"addresses":[],"topics":[]},"wss_url":"wss:link","name":"eth_logs"}}"#.to_owned(),
            name: "eth_connection".to_owned(),
            db_type: "ethereum".to_owned(),
            ..Default::default()
        },
        DBType::Events => DbConnection {
            auth: r#"{"Events":{"database":"users"}}"#.to_owned(),
            name: "events_connection".to_owned(),
            db_type: "events".to_owned(),
            ..Default::default()
        },
        DBType::Snowflake => DbConnection {
            auth: r#"{"Snowflake":{"server":"tx06321.eu-north-1.aws.snowflakecomputing.com","port":"443","user":"karolisgud","password":"uQ@8S4856G9SHP6","database":"DOZER_SNOWFLAKE_SAMPLE_DATA","schema":"PUBLIC","warehouse":"TEST","driver":"{/opt/snowflake/snowflakeodbc/lib/universal/libSnowflake.dylib}"}}"#.to_owned(),
            name: "snowflake_connection".to_owned(),
            db_type: "snowflake".to_owned(),
            ..Default::default()
        },
        DBType::Kafka => DbConnection {
            auth: r#"{"Kafka":{"broker":"localhost:9092","topic":"dbserver1.public.products"}}"#.to_owned(),
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
    let db_types = get_db_type_supported();
    db_types.iter().enumerate().for_each(|(idx, db_type)| {
        let db_connection = fake_dbconnection(db_type.to_owned());
        if idx < config_id.connection_ids.len() {
            insert_connections(
                connection,
                config_id.connection_ids[idx].to_owned(),
                config_id.app_id.to_owned(),
                db_connection.db_type,
                db_connection.auth,
                db_connection.name,
            );
            insert_sources(
                connection,
                config_id.source_ids[idx].to_owned(),
                config_id.app_id.to_owned(),
                format!("source_{:}", db_type.to_owned()),
                format!("table_source_{:}", db_type.to_owned()),
                config_id.connection_ids[idx].to_owned(),
                "id".to_owned(),
            )
        }
    });

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
    columns: String,
) {
    diesel::sql_query(
        format!("INSERT INTO sources (id, app_id, name, table_name, connection_id,columns, created_at, updated_at) VALUES('{}', '{}', '{}', '{}', '{}','{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);",
        source_id, app_id, name,table_name, connection_id, columns))
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
        format!("INSERT INTO endpoints (id, app_id, name, \"path\", \"sql\", primary_keys, created_at, updated_at) VALUES( '{}', '{}', '{}', '{}','{}','{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);"
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
