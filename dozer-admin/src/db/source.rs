use super::persistable::Persistable;
use super::pool::DbPool;
use super::schema::{self, sources};
use diesel::{ insert_into, ExpressionMethods, Connection};
use diesel::{prelude::*};
use dozer_orchestrator::orchestration::models::{self,source::Source};
use dozer_orchestrator::orchestration::models::source::{HistoryType, RefreshConfig};
use schema::sources::dsl::*;
use std::error::Error;

#[derive(Queryable, PartialEq, Debug, Clone)]
#[diesel(table_name = sources)]
struct DBSource {
    id: String,
    name: String,
    dest_table_name: String,
    source_table_name: String,
    connection_id: String,
    history_type: String,
    refresh_config: String,
    created_at: String,
    updated_at: String,
}
#[derive(Insertable, PartialEq, Debug, Clone)]
#[diesel(table_name = sources)]
pub struct NewSource {
    name: String,
    dest_table_name: String,
    source_table_name: String,
    connection_id: String,
    history_type: String,
    refresh_config: String,
    id: String,
}

impl Persistable<'_, Source> for Source {
    fn save(&mut self, pool: DbPool) -> Result<&mut Source, Box<dyn Error>> {
        let history_type_value = serde_json::to_string(&self.history_type)?;
        let refresh_config_value = serde_json::to_string(&self.refresh_config)?;
        let db = &mut pool.get()?;
        let generated_id = uuid::Uuid::new_v4().to_string();
        let current_source = self.clone();
        let mut cloned_connection = self.connection.clone();
        db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
            if let None = current_source.connection.id {
                cloned_connection.save(pool)?;
            }
            let valid_connection_id = cloned_connection.id;
            let new_source = NewSource {
                name: current_source.name.clone(),
                dest_table_name: current_source.dest_table_name.clone(),
                source_table_name: current_source.source_table_name.clone(),
                connection_id: valid_connection_id.unwrap(),
                history_type: history_type_value,
                refresh_config: refresh_config_value,
                id: generated_id.clone(),
            };
            insert_into(sources).values(&new_source).execute(conn)?;
           return Ok(());
        })?;
        self.id = Some(generated_id);
        return Ok(self);
    }

    fn get_by_id(pool: DbPool, input_id: String) -> Result<Source, Box<dyn Error>> {
        let mut db = pool.get()?;
        let db_source: DBSource = sources
            .filter(sources::columns::id.eq(input_id))
            .first(&mut db)?;
        let connection = models::connection::Connection::get_by_id(pool, db_source.connection_id)?;
        let history_type_value: HistoryType = serde_json::from_str(&db_source.history_type)?;
        let refresh_config_value: RefreshConfig = serde_json::from_str(&db_source.refresh_config)?;
        return Ok(Source {
            id: Some(db_source.id),
            name: db_source.name,
            dest_table_name: db_source.dest_table_name,
            source_table_name: db_source.source_table_name,
            connection,
            history_type: history_type_value,
            refresh_config: refresh_config_value,
        });
    }

    fn get_multiple(_pool: DbPool) -> Result<Vec<Source>, Box<dyn Error>> {
        todo!();
    }

    fn update(_pool: DbPool) -> Result<Source, Box<dyn Error>> {
        todo!()
    }
}
