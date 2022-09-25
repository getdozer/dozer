use std::error::Error;
use std::str::FromStr;
use super::pool::DbPool;
use super::schema::{self};
use crate::db::schema::connections::dsl::connections;
use diesel::insert_into;
use diesel::prelude::*;
use dozer_orchestrator::orchestration::models::connection::{Authentication, Connection, DBType};
use dozer_orchestrator::orchestration::models::source::Source;
use schema::connections::dsl::*;
use serde::Serialize;

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
pub trait Persistable<T: Serialize> {
    fn save(&mut self, pool: DbPool) -> Result<&mut T, Box<dyn Error>>;
    fn get_by_id(pool: DbPool, input_id: String) -> Result<T, Box<dyn Error>>;
    fn get_multiple(pool: DbPool) -> Result<Vec<T>, Box<dyn Error>>;
}
impl Persistable<Source> for Source {
    fn save(&mut self, _pool: DbPool) -> Result<&mut Source, Box<dyn Error>> {
        todo!()
    }

    fn get_by_id(_pool: DbPool, _input_id: String) -> Result<Source, Box<dyn Error>> {
        todo!()
    }

    fn get_multiple(_pool: DbPool) -> Result<Vec<Source>, Box<dyn Error>> {
        todo!()
    }
}
impl Persistable<Connection> for Connection {
    fn save(&mut self, pool: DbPool) -> Result<&mut Self, Box<dyn Error>> {
        let mut db = pool.get()?;
        let auth_value = serde_json::to_value(&self.authentication)
            .map_err(|err| string_to_static_str(err.to_string()))?;
        let generated_id = uuid::Uuid::new_v4().to_string();
        let id_value: &String = self.id.as_ref().unwrap_or_else(|| &generated_id);
        let _inserted_rows = insert_into(connections)
            .values((
                auth.eq(auth_value.to_string()),
                db_type.eq(self.db_type.to_string()),
                name.eq(&self.name),
                id.eq(id_value.clone()),
            ))
            .execute(&mut db);
        self.id = Some(id_value.clone());
        return Ok(self);
    }

    fn get_by_id(pool: DbPool, connection_id: String) -> Result<Connection, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result = connections
            .filter(id.eq(connection_id))
            .first::<(String, String, String, String)>(&mut db)?;
        let auth_value = serde_json::from_str::<Authentication>(&result.1)
            .map_err(|err| string_to_static_str(err.to_string()))?;
        let db_type_value = DBType::from_str(&result.3)?;
        return Ok(Connection {
            db_type: db_type_value,
            authentication: auth_value,
            name: result.2,
            id: Some(result.0),
        });
    }

    fn get_multiple(pool: DbPool) -> Result<Vec<Connection>, Box<dyn Error>> {
        let mut db = pool.get()?;
        let results = connections.load::<(String, String, String, String)>(&mut db)?;
        let response: Vec<Connection> = results
            .iter()
            .map(|result| -> Connection {
                let auth_value = serde_json::from_str::<Authentication>(&result.1)
                    .map_err(|err| string_to_static_str(err.to_string()));
                let db_type_value = DBType::from_str(&result.3);
                return Connection {
                    db_type: db_type_value.unwrap(),
                    authentication: auth_value.unwrap(),
                    name: result.2.clone(),
                    id: Some(result.0.clone()),
                };
            })
            .collect();

        return Ok(response);
    }
}
