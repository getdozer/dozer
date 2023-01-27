use super::{
    application::Application,
    connection::{DbConnection, NewConnection},
    constants,
    persistable::Persistable,
    pool::DbPool,
    schema::{self, connections, sources},
};
use crate::db::schema::apps::dsl::apps;
use crate::db::schema::connections::dsl::connections as dsl_connections;

use crate::server::dozer_admin_grpc::Pagination;
use diesel::{insert_into, prelude::*, query_dsl::methods::FilterDsl, ExpressionMethods};
use dozer_types::serde;
use schema::sources::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(
    Queryable,
    Identifiable,
    Associations,
    PartialEq,
    Eq,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    Default,
)]
#[diesel(belongs_to(Application, foreign_key = app_id))]
#[diesel(table_name = sources)]
pub struct DBSource {
    pub(crate) id: String,
    pub(crate) app_id: String,
    pub(crate) name: String,
    pub(crate) table_name: String,
    pub(crate) connection_id: String,
    pub(crate) columns: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Eq, Debug, Clone)]
#[diesel(table_name = sources)]
pub struct NewSource {
    id: String,
    app_id: String,
    name: String,
    table_name: String,
    connection_id: String,
    columns_: String,
}

fn convert_to_source(
    db_source: DBSource,
    connection: dozer_types::models::connection::Connection,
) -> dozer_types::models::source::Source {
    let columns_value: Vec<String> = db_source
        .columns
        .split(',')
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    dozer_types::models::source::Source {
        id: Some(db_source.id),
        app_id: Some(db_source.app_id),
        name: db_source.name,
        table_name: db_source.table_name,
        columns: columns_value,
        connection: Some(connection),
        refresh_config: Some(dozer_types::models::source::RefreshConfig::default()),
    }
}
impl Persistable<'_, dozer_types::models::source::Source> for dozer_types::models::source::Source {
    fn save(
        &mut self,
        pool: DbPool,
    ) -> Result<&mut dozer_types::models::source::Source, Box<dyn Error>> {
        self.upsert(pool)
    }

    fn by_id(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<dozer_types::models::source::Source, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DBSource = FilterDsl::filter(
            FilterDsl::filter(sources, id.eq(input_id)),
            app_id.eq(application_id.to_owned()),
        )
        .first(&mut db)?;
        let connection_info = dozer_types::models::connection::Connection::by_id(
            pool,
            result.to_owned().connection_id,
            application_id,
        )?;
        let source_info = convert_to_source(result, connection_info);
        Ok(source_info)
    }

    fn list(
        pool: DbPool,
        application_id: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<dozer_types::models::source::Source>, Pagination), Box<dyn Error>> {
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
        let response: Vec<dozer_types::models::source::Source> = results
            .iter()
            .map(|result| {
                convert_to_source(
                    result.to_owned().0,
                    dozer_types::models::connection::Connection::try_from(result.to_owned().1)
                        .unwrap(),
                )
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

    fn upsert(
        &mut self,
        pool: DbPool,
    ) -> Result<&mut dozer_types::models::source::Source, Box<dyn Error>> {
        let mut db = pool.get()?;
        if let Some(connection) = self.connection.to_owned() {
            let mut connection = connection;
            db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
                let _ = apps
                    .find(self.app_id.to_owned().unwrap_or_default())
                    .first::<Application>(conn)
                    .map_err(|err| format!("App_id: {err:}"))?;

                connection.app_id = self.app_id.to_owned();
                let new_connection = NewConnection::try_from(connection.to_owned())?;

                // upsert connection incase connection not created
                let _inserted_connection = insert_into(dsl_connections)
                    .values(&new_connection)
                    .on_conflict(connections::id)
                    .do_update()
                    .set(&new_connection)
                    .execute(conn);
                self.connection = Some(connection.to_owned());

                let new_source = NewSource {
                    name: self.name.to_owned(),
                    table_name: self.table_name.to_owned(),
                    connection_id: connection.to_owned().id.unwrap_or_default(),
                    id: self.id.to_owned().unwrap_or_default(),
                    app_id: self.app_id.to_owned().unwrap_or_default(),
                    columns_: self.columns.join(","),
                };
                insert_into(sources)
                    .values(&new_source)
                    .on_conflict(sources::id)
                    .do_update()
                    .set(&new_source)
                    .execute(conn)?;
                self.id = Some(new_source.id);
                Ok(())
            })?;
            Ok(self)
        } else {
            Err("Missing connection info for Source".to_owned())?
        }
    }

    fn delete(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<bool, Box<dyn Error>> {
        let mut db = pool.get()?;
        diesel::delete(FilterDsl::filter(
            FilterDsl::filter(sources, id.eq(input_id)),
            app_id.eq(application_id),
        ))
        .execute(&mut db)?;
        Ok(true)
    }
}
