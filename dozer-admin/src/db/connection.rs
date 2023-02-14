use super::schema::connections;
use diesel::prelude::*;
use dozer_types::{
    models::connection::{Authentication, DBType},
    serde,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, str::FromStr};
#[derive(Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = connections)]
pub struct DbConnection {
    pub(crate) id: String,
    pub(crate) auth: String,
    pub(crate) name: String,
    pub(crate) db_type: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[diesel(table_name = connections)]
pub struct NewConnection {
    pub(crate) id: Option<String>,
    pub(crate) auth: String,
    pub(crate) name: String,
    pub(crate) db_type: String,
}

impl NewConnection {
    pub fn from(
        connection: dozer_types::models::connection::Connection,
        id_str: String,
    ) -> Result<Self, Box<dyn Error>> {
        let auth_string = serde_json::to_string(&connection.authentication)?;
        let db_type_value = DBType::try_from(connection.db_type)?;
        Ok(NewConnection {
            auth: auth_string,
            name: connection.name,
            db_type: db_type_value.as_str_name().to_owned(),
            id: Some(id_str),
        })
    }
}
impl TryFrom<DbConnection> for dozer_types::models::connection::Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: DbConnection) -> Result<Self, Self::Error> {
        let db_type_value: DBType = DBType::from_str(&item.db_type)?;
        let auth_value: Authentication = serde_json::from_str(&item.auth)?;
        Ok(dozer_types::models::connection::Connection {
            db_type: db_type_value as i32,
            name: item.name,
            authentication: Some(auth_value),
        })
    }
}
