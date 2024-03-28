use dozer_ingestion_connector::async_trait;
use dozer_ingestion_mongodb::{
    bson::{self, doc},
    mongodb::{
        self,
        options::{ClientOptions, InsertOneOptions, WriteConcern},
    },
    MongodbConnector,
};
use dozer_utils::{process::run_docker_compose, Cleanup};
use tempfile::TempDir;

use crate::test_suite::DataReadyConnectorTest;

pub struct MongodbConnectorTest {
    _cleanup: Cleanup,
    _temp_dir: TempDir,
}

#[async_trait]
impl DataReadyConnectorTest for MongodbConnectorTest {
    type Connector = MongodbConnector;

    async fn new() -> (Self, Self::Connector) {
        let (db, connector_test, connector) = create_mongodb_server().await;
        create_collection_with_all_supported_data_types(&db, "test_collection").await;
        (connector_test, connector)
    }
}

async fn create_collection(
    database: &mongodb::Database,
    name: &str,
) -> mongodb::Collection<bson::Document> {
    database
        .run_command(
            doc! {
                "create": name,
                "changeStreamPreAndPostImages": {"enabled": true}
            },
            None,
        )
        .await
        .expect("Failed to create collection");
    database.collection(name)
}

async fn create_collection_with_all_supported_data_types(database: &mongodb::Database, name: &str) {
    let collection = create_collection(database, name).await;
    collection
        .insert_one(
            doc! {
                    "null": null,
                    "bool": true,
                    "float": 3.,
                    "int": 1,
                    "string": "string",
                    "array": [1, {"a": 1}, [1, 2], "b"],
                    "object": {"a": 1, "b": [1, 2]}
            },
            Some(
                InsertOneOptions::builder()
                    .write_concern(Some(WriteConcern::MAJORITY))
                    .build(),
            ),
        )
        .await
        .expect("Failed to insert into collection");
}

async fn create_mongodb_server() -> (mongodb::Database, MongodbConnectorTest, MongodbConnector) {
    let database = "testdb".to_owned();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let docker_compose_path = temp_dir.path().join("docker-compose.yaml");
    std::fs::write(&docker_compose_path, DOCKER_COMPOSE_YAML)
        .expect("Failed to write docker compose file");

    let cleanup = run_docker_compose(&docker_compose_path, "dozer-wait-for-connections-healthy");

    let connection_string = format!("mongodb://localhost:27018/{database}");

    let connection_options = ClientOptions::parse(&connection_string).await.unwrap();
    let mut initiate_connection_options = connection_options.clone();
    initiate_connection_options.direct_connection = Some(true);
    // We need a direct connection, as our singular node is not yet initialized as a replSet,
    // but it was started as one
    let client = mongodb::Client::with_options(initiate_connection_options).unwrap();

    client
        .database("admin")
        .run_command(
            doc! {
                    "replSetInitiate": {
                        "_id": "rs0",
                        "members": [
                            {"_id": 0, "host": "localhost:27018"}
                        ]
                    },
            },
            None,
        )
        .await
        .expect("Failed to initialize replSet");

    let client = mongodb::Client::with_options(connection_options.clone()).unwrap();
    let db = client.default_database().unwrap();
    let connector = MongodbConnector::new(connection_string).unwrap();
    let test = MongodbConnectorTest {
        _cleanup: cleanup,
        _temp_dir: temp_dir,
    };
    (db, test, connector)
}

const DOCKER_COMPOSE_YAML: &str = r#"version: '2.4'
services:
  mongodb:
    container_name: dozer-connectors-mongodb
    image: mongodb/mongodb-community-server:6.0.7-ubi8
    ports:
    - target: 27018
      published: 27018
    command: mongod --replSet rs0 --port 27018 --bind_ip localhost,mongodb
    healthcheck:
      test:
      - CMD-SHELL
      - mongosh --host "localhost:27018" --eval 'print("ready")'
      interval: 5s
      timeout: 5s
      retries: 5
  dozer-wait-for-connections-healthy:
    image: alpine
    command: echo 'All connections are healthy'
    depends_on:
      mongodb:
        condition: service_healthy
"#;
