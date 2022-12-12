use super::{
    application::Application,
    constants,
    persistable::Persistable,
    pool::DbPool,
    schema::{self, connections},
};
use crate::db::schema::apps::dsl::apps;
use crate::server::dozer_admin_grpc::Pagination;
use diesel::{insert_into, prelude::*, query_dsl::methods::FilterDsl, ExpressionMethods};
use dozer_types::{
    models::connection::{Authentication, DBType},
    serde,
};
use schema::connections::dsl::*;
use serde::{Deserialize, Serialize};
use std::{error::Error, str::FromStr};
#[derive(Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = connections)]
pub struct DbConnection {
    pub(crate) id: String,
    pub(crate) app_id: String,
    pub(crate) auth: String,
    pub(crate) name: String,
    pub(crate) db_type: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = connections)]
struct NewConnection {
    pub(crate) auth: String,
    pub(crate) app_id: String,
    pub(crate) name: String,
    pub(crate) db_type: String,
    pub(crate) id: String,
}

impl TryFrom<DbConnection> for dozer_types::models::connection::Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: DbConnection) -> Result<Self, Self::Error> {
        let db_type_value: DBType = DBType::from_str(&item.db_type)?;
        let auth_value: Authentication = serde_json::from_str(&item.auth)?;
        Ok(dozer_types::models::connection::Connection {
            id: Some(item.id),
            app_id: Some(item.app_id),
            db_type: db_type_value as i32,
            name: item.name,
            authentication: Some(auth_value),
        })
    }
}
impl TryFrom<dozer_types::models::connection::Connection> for NewConnection {
    type Error = Box<dyn Error>;
    fn try_from(item: dozer_types::models::connection::Connection) -> Result<Self, Self::Error> {
        let auth_string = serde_json::to_string(&item.authentication)?;
        let db_type_value = DBType::try_from(item.db_type)?;
        Ok(NewConnection {
            auth: auth_string,
            app_id: item.app_id.unwrap_or_default(),
            name: item.name,
            db_type: db_type_value.as_str_name().to_owned(),
            id: item.id.unwrap_or_default(),
        })
    }
}

impl Persistable<'_, dozer_types::models::connection::Connection>
    for dozer_types::models::connection::Connection
{
    fn save(
        &mut self,
        pool: DbPool,
    ) -> Result<&mut dozer_types::models::connection::Connection, Box<dyn Error>> {
        self.upsert(pool)
    }

    fn by_id(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<dozer_types::models::connection::Connection, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DbConnection = FilterDsl::filter(
            FilterDsl::filter(connections, id.eq(input_id)),
            app_id.eq(application_id),
        )
        .first(&mut db)?;
        Ok(dozer_types::models::connection::Connection::try_from(result).unwrap())
    }

    fn list(
        pool: DbPool,
        application_id: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<dozer_types::models::connection::Connection>, Pagination), Box<dyn Error>>
    {
        let mut db = pool.get()?;
        let offset = offset.unwrap_or(constants::OFFSET);
        let limit = limit.unwrap_or(constants::LIMIT);
        let filter_dsl = FilterDsl::filter(connections, app_id.eq(application_id));
        let results: Vec<DbConnection> = filter_dsl
            .to_owned()
            .offset(offset.into())
            .order_by(connections::id.asc())
            .limit(limit.into())
            .load(&mut db)?;
        let total: i64 = filter_dsl.count().get_result(&mut db)?;
        let connection_info: Vec<dozer_types::models::connection::Connection> = results
            .iter()
            .map(|result| {
                dozer_types::models::connection::Connection::try_from(result.clone()).unwrap()
            })
            .collect();
        Ok((
            connection_info,
            Pagination {
                limit,
                total: total.try_into().unwrap_or(0),
                offset,
            },
        ))
    }

    fn upsert(
        &mut self,
        pool: DbPool,
    ) -> Result<&mut dozer_types::models::connection::Connection, Box<dyn Error>> {
        let new_connection = NewConnection::try_from(self.clone())?;
        let mut db = pool.get()?;
        db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
            let _ = apps
                .find(new_connection.app_id.to_owned())
                .first::<Application>(conn)
                .map_err(|err| format!("App_id: {:}", err))?;
            let _inserted = insert_into(connections)
                .values(&new_connection)
                .on_conflict(connections::id)
                .do_update()
                .set(&new_connection)
                .execute(conn);
            self.id = Some(new_connection.id);
            Ok(())
        })?;

        Ok(self)
    }

    fn delete(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<bool, Box<dyn Error>> {
        let mut db = pool.get()?;
        diesel::delete(FilterDsl::filter(
            FilterDsl::filter(connections, id.eq(input_id)),
            app_id.eq(application_id),
        ))
        .execute(&mut db)?;
        Ok(true)
    }
}
