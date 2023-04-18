use super::schema::connections;
use diesel::prelude::*;
use dozer_types::{models::connection::ConnectionConfig, serde};
use serde::{Deserialize, Serialize};
use std::error::Error;
#[derive(Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = connections)]
pub struct DbConnection {
    pub(crate) id: String,
    pub(crate) config: String,
    pub(crate) name: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[diesel(table_name = connections)]
pub struct NewConnection {
    pub(crate) id: String,
    pub(crate) config: String,
    pub(crate) name: String,
}

impl NewConnection {
    pub fn from(
        connection: dozer_types::models::connection::Connection,
        id: String,
    ) -> Result<Self, Box<dyn Error>> {
        let config_str = serde_json::to_string(&connection.config)?;
        Ok(NewConnection {
            config: config_str,
            name: connection.name,
            id,
        })
    }
}
impl TryFrom<DbConnection> for dozer_types::models::connection::Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: DbConnection) -> Result<Self, Self::Error> {
        let conn_config: ConnectionConfig = serde_json::from_str(&item.config)?;
        Ok(dozer_types::models::connection::Connection {
            name: item.name,
            config: Some(conn_config),
        })
    }
}
