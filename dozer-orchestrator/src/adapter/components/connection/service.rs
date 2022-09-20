use crate::adapter::db::{db_persistent_trait::DbPersistentTrait, models as DBModels};
use dozer_ingestion::connectors::{
    connector::Connector,
    postgres::connector::{PostgresConfig, PostgresConnector},
};
use serde_json::Value;

use super::{dal::ConnectionDbSvc, traits::ConnectionSvcTrait};
pub struct ConnectionSvc {
    connection_db_svc: ConnectionDbSvc,
}
impl ConnectionSvc {
    pub fn new(database_url: String) -> Self {
        let connection_db_svc = ConnectionDbSvc::new(database_url);
        Self { connection_db_svc }
    }
}
impl ConnectionSvcTrait<PostgresConfig, DBModels::connection::Connection> for ConnectionSvc {
    fn get_connector(&self, config: PostgresConfig) -> Box<dyn Connector> {
        Box::new(PostgresConnector::new(config))
    }

    fn get_db_svc(&self) -> Box<dyn DbPersistentTrait<DBModels::connection::Connection>> {
        Box::new(self.connection_db_svc.clone())
    }

    fn convert_connection_to_config(
        &self,
        input: DBModels::connection::Connection,
    ) -> PostgresConfig {
        let postgres_auth: Value = serde_json::from_str(&input.auth).unwrap();
        let port = postgres_auth["port"]
            .as_str()
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let conn_str = format!(
            "host={} port={} user={} dbname={} password={}",
            postgres_auth["host"],
            port,
            postgres_auth["user"],
            postgres_auth["database"],
            postgres_auth["password"],
        );
        let postgres_config = PostgresConfig {
            name: postgres_auth["name"].to_string(),
            conn_str: conn_str.clone(),
            tables: None,
        };
        postgres_config
    }
}
