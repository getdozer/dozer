use super::persistable::Persistable;
use super::pool::DbPool;
use super::schema::{self, connections};
use diesel::{ prelude::*};
use diesel::{insert_into, ExpressionMethods};
use dozer_orchestrator::orchestration::models::connection::{Authentication, Connection, DBType};
use schema::connections::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::str::FromStr;
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
#[derive(Insertable, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = connections)]
struct NewConnection {
    auth: String,
    name: String,
    db_type: String,
    id: String,
}
impl TryFrom<DbConnection> for Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: DbConnection) -> Result<Self, Self::Error> {
        let db_type_value = DBType::from_str(&item.db_type)?;
        let authentication:Authentication = serde_json::from_str(&item.auth)?;
        return Ok(Connection {
            db_type:db_type_value,
            authentication: authentication,
            name: item.name,
            id: Some(item.id),
        });
    }
}
impl Persistable<'_, Connection> for Connection {
    fn save(&mut self, pool: DbPool) -> Result<&mut Self, Box<dyn Error>> {
        let authentication_serialized = serde_json::to_string(&self.authentication)?;
        let generated_id = uuid::Uuid::new_v4().to_string();
        let new_connection = NewConnection {
            auth: authentication_serialized,
            name: self.name.clone(),
            db_type: DBType::to_string(&self.db_type),
            id: generated_id.clone(),
        };
        let mut db = pool.get()?;
        let _inserted = insert_into(connections)
            .values(&new_connection)
            .execute(&mut db);
        self.id = Some(generated_id);
        return Ok(self);
    }

    fn get_by_id(pool: DbPool, connection_id: String) -> Result<Connection, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DbConnection = connections.filter(id.eq(connection_id)).first(&mut db)?;
        let connection = Connection::try_from(result);
        return connection;
    }

    fn get_multiple(pool: DbPool) -> Result<Vec<Connection>, Box<dyn Error>> {
        let mut db = pool.get()?;
        let results: Vec<DbConnection> = connections
            .offset(0)
            .order_by(connections::id.asc())
            .limit(100)
            .load(&mut db)?;

        let response = results
            .iter()
            .map(|result| {
                return Connection::try_from(result.clone()).unwrap();
            })
            .collect();
        return Ok(response);
    }

    fn update(pool: DbPool) -> Result<Connection, Box<dyn Error>> {
        todo!()
    }
}
