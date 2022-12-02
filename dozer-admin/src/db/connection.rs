use super::{
    application::Application,
    constants,
    persistable::Persistable,
    pool::DbPool,
    schema::{self, connections},
};
use crate::db::schema::apps::dsl::apps;
use crate::server::dozer_admin_grpc::{self, ConnectionInfo, ConnectionType, Pagination};
use diesel::{insert_into, prelude::*, query_dsl::methods::FilterDsl, ExpressionMethods};
use dozer_types::serde;
use schema::connections::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
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

impl TryFrom<DbConnection> for ConnectionInfo {
    type Error = Box<dyn Error>;
    fn try_from(item: DbConnection) -> Result<Self, Self::Error> {
        let db_type_value: ConnectionType = ConnectionType::try_from(item.db_type.clone())?;
        let auth_value: dozer_admin_grpc::Authentication = serde_json::from_str(&item.auth)?;
        Ok(ConnectionInfo {
            id: item.id,
            app_id: item.app_id,
            name: item.name,
            r#type: db_type_value as i32,
            authentication: Some(auth_value),
        })
    }
}
impl TryFrom<i32> for ConnectionType {
    type Error = Box<dyn Error>;
    fn try_from(item: i32) -> Result<Self, Self::Error> {
        match item {
            0 => Ok(ConnectionType::Postgres),
            1 => Ok(ConnectionType::Snowflake),
            2 => Ok(ConnectionType::Databricks),
            3 => Ok(ConnectionType::Eth),
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
            "eth" => Ok(ConnectionType::Eth),
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
        Ok(NewConnection {
            auth: auth_string,
            app_id: item.app_id,
            name: item.name,
            db_type: connection_type_string.to_owned(),
            id: item.id,
        })
    }
}

impl Persistable<'_, ConnectionInfo> for ConnectionInfo {
    fn save(&mut self, pool: DbPool) -> Result<&mut ConnectionInfo, Box<dyn Error>> {
        self.upsert(pool)
    }

    fn by_id(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<ConnectionInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DbConnection = FilterDsl::filter(
            FilterDsl::filter(connections, id.eq(input_id)),
            app_id.eq(application_id),
        )
        .first(&mut db)?;
        Ok(ConnectionInfo::try_from(result).unwrap())
    }

    fn list(
        pool: DbPool,
        application_id: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<ConnectionInfo>, Pagination), Box<dyn Error>> {
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
        let connection_info: Vec<ConnectionInfo> = results
            .iter()
            .map(|result| ConnectionInfo::try_from(result.clone()).unwrap())
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

    fn upsert(&mut self, pool: DbPool) -> Result<&mut ConnectionInfo, Box<dyn Error>> {
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
            self.id = new_connection.id;
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
