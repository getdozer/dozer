use crate::server::dozer_admin_grpc::{ConnectionInfo, ConnectionType, self};
use super::{persistable::Persistable, pool::DbPool, schema::{self, connections}};
use diesel::{prelude::*, insert_into, ExpressionMethods};
use schema::connections::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
#[derive(Queryable, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = connections)]
struct DbConnection {
    id: String,
    auth: String,
    name: String,
    db_type: String,
    created_at: String,
    updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = connections)]
struct NewConnection {
    auth: String,
    name: String,
    db_type: String,
    id: String,
}

impl TryFrom<DbConnection> for ConnectionInfo {
    type Error = Box<dyn Error>;
    fn try_from(item: DbConnection) -> Result<Self, Self::Error> {
        let db_type_value: ConnectionType = ConnectionType::try_from(item.db_type.clone())?;
        let auth_value: dozer_admin_grpc::Authentication = serde_json::from_str(&item.auth)?;

        return Ok(ConnectionInfo {
            id: Some(item.id),
            name: item.name,
            r#type: db_type_value as i32,
            authentication: Some(auth_value),
        });
    }
}
impl TryFrom<i32> for ConnectionType {
    type Error = Box<dyn Error>;
    fn try_from(item: i32) -> Result<Self, Self::Error> {
        match item {
            0 => Ok(ConnectionType::Postgres),
            1 => Ok(ConnectionType::Snowflake),
            2 => Ok(ConnectionType::Databricks),
            _ => Err("ConnectionType enum not match".to_owned())?,
        }
    }
}
impl TryFrom<String> for ConnectionType {
    type Error = Box<dyn Error>;
    fn try_from(item: String) -> Result<Self, Self::Error> {
        match item.to_lowercase().as_str() {
            "postgres" => Ok(ConnectionType::Postgres),
            "snowflake" => Ok(ConnectionType::Snowflake),
            "databricks" => Ok(ConnectionType::Databricks),
            _ => Err("String not match ConnectionType".to_owned())?,
        }
    }
}
impl TryFrom<ConnectionInfo> for NewConnection {
    type Error = Box<dyn Error>;
    fn try_from(item: ConnectionInfo) -> Result<Self, Self::Error> {
        let auth_string = serde_json::to_string(&item.authentication)?;
        let connection_type = ConnectionType::try_from(item.r#type)?;
        let connection_type_string = connection_type.as_str_name();
        let generated_id = uuid::Uuid::new_v4().to_string();
        let connetion_id = item.id.unwrap_or(generated_id);
        Ok(NewConnection {
            auth: auth_string,
            name: item.name,
            db_type: connection_type_string.to_owned(),
            id: connetion_id,
        })
    }
}
impl Persistable<'_, ConnectionInfo> for ConnectionInfo {
    fn save(&mut self, pool: DbPool) -> Result<&mut ConnectionInfo, Box<dyn Error>> {
        let new_connection = NewConnection::try_from(self.clone())?;
        let mut db = pool.get()?;
        let _inserted = insert_into(connections)
            .values(&new_connection)
            .on_conflict(connections::id)
            .do_update()
            .set(&new_connection)
            .execute(&mut db);
        self.id = Some(new_connection.id);
        return Ok(self);
    }

    fn get_by_id(pool: DbPool, input_id: String) -> Result<ConnectionInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DbConnection = connections.filter(id.eq(input_id)).first(&mut db)?;
        let connection = ConnectionInfo::try_from(result);
        return connection;
    }

    fn get_multiple(pool: DbPool) -> Result<Vec<ConnectionInfo>, Box<dyn Error>> {
        let mut db = pool.get()?;
        let results: Vec<DbConnection> = connections
            .offset(0)
            .order_by(connections::id.asc())
            .limit(100)
            .load(&mut db)?;

        let response = results
            .iter()
            .map(|result| {
                return ConnectionInfo::try_from(result.clone()).unwrap();
            })
            .collect();
        return Ok(response);
    }

    fn upsert(&mut self, pool: DbPool) -> Result<&mut ConnectionInfo, Box<dyn Error>> {
        let new_connection = NewConnection::try_from(self.clone())?;
        let mut db = pool.get()?;
        let _inserted = insert_into(connections)
            .values(&new_connection)
            .on_conflict(connections::id)
            .do_update()
            .set(&new_connection)
            .execute(&mut db);
        self.id = Some(new_connection.id);
        return Ok(self);
    }
}
