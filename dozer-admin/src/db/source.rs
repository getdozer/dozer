use super::{
    application::Application,
    connection::DbConnection,
    constants,
    persistable::Persistable,
    pool::DbPool,
    schema::{self, connections, sources},
};
use crate::db::schema::apps::dsl::apps;
use crate::server::dozer_admin_grpc::{ConnectionInfo, Pagination, SourceInfo};
use diesel::{insert_into, prelude::*, query_dsl::methods::FilterDsl, ExpressionMethods};
use dozer_types::serde;
use schema::sources::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(
    Queryable, Identifiable, Associations, PartialEq, Eq, Debug, Clone, Serialize, Deserialize,
)]
#[diesel(belongs_to(Application, foreign_key = app_id))]
#[diesel(table_name = sources)]
pub struct DBSource {
    pub id: String,
    pub app_id: String,
    pub name: String,
    pub table_name: String,
    connection_id: String,
    created_at: String,
    updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Eq, Debug, Clone)]
#[diesel(table_name = sources)]
pub struct NewSource {
    name: String,
    app_id: String,
    table_name: String,
    connection_id: String,
    id: String,
}
impl Persistable<'_, SourceInfo> for SourceInfo {
    fn save(&mut self, pool: DbPool) -> Result<&mut SourceInfo, Box<dyn Error>> {
        self.upsert(pool)
    }

    fn by_id(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<SourceInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DBSource = FilterDsl::filter(
            FilterDsl::filter(sources, id.eq(input_id)),
            app_id.eq(application_id.to_owned()),
        )
        .first(&mut db)?;
        let connection_info = ConnectionInfo::by_id(pool, result.connection_id, application_id)?;
        let source_info = SourceInfo {
            id: result.id,
            app_id: result.app_id,
            name: result.name,
            table_name: result.table_name,
            connection: Some(connection_info),
        };
        Ok(source_info)
    }

    fn list(
        pool: DbPool,
        application_id: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<SourceInfo>, Pagination), Box<dyn Error>> {
        let offset = offset.unwrap_or(constants::OFFSET);
        let limit = limit.unwrap_or(constants::LIMIT);
        let mut db = pool.get()?;
        let filter_dsl = FilterDsl::filter(sources, app_id.eq(application_id.to_owned()));
        let results: Vec<(DBSource, DbConnection)> = FilterDsl::filter(
            sources::table
                .inner_join(connections::table)
                .select((sources::all_columns, connections::all_columns)),
            sources::app_id.eq(application_id),
        )
        .load::<(DBSource, DbConnection)>(&mut db)?;
        let total: i64 = filter_dsl.count().get_result(&mut db)?;
        let response: Vec<SourceInfo> = results
            .iter()
            .map(|result| SourceInfo {
                app_id: result.0.app_id.to_owned(),
                connection: Some(ConnectionInfo::try_from(result.1.to_owned()).unwrap()),
                table_name: result.0.table_name.to_owned(),
                id: result.0.id.to_owned(),
                name: result.0.name.to_owned(),
            })
            .collect();
        Ok((
            response,
            Pagination {
                limit,
                total: total.try_into().unwrap(),
                offset,
            },
        ))
    }

    fn upsert(&mut self, pool: DbPool) -> Result<&mut SourceInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        if let Some(connection) = self.connection.to_owned() {
            let mut connection = connection;
            let _ = apps
                .find(self.app_id.to_owned())
                .first::<Application>(&mut db)
                .map_err(|err| format!("App_id: {:}", err))?;
            db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
                connection.upsert(pool.to_owned())?;
                self.connection = Some(connection.to_owned());
                let new_source = NewSource {
                    name: self.name.to_owned(),
                    table_name: self.table_name.to_owned(),
                    connection_id: connection.to_owned().id,
                    id: self.id.to_owned(),
                    app_id: self.app_id.to_owned(),
                };
                insert_into(sources)
                    .values(&new_source)
                    .on_conflict(sources::id)
                    .do_update()
                    .set(&new_source)
                    .execute(conn)?;
                self.id = new_source.id;
                Ok(())
            })?;
            Ok(self)
        } else {
            Err("Missing connection info for Source".to_owned())?
        }
    }
}
