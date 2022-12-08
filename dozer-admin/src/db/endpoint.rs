use super::{
    application::Application,
    constants,
    persistable::Persistable,
    pool::DbPool,
    schema::{self, endpoints, source_endpoints},
};
use crate::db::schema::apps::dsl::apps;
use crate::server::dozer_admin_grpc::{EndpointInfo, Pagination};
use diesel::{insert_into, prelude::*, query_dsl::methods::FilterDsl, ExpressionMethods};
use schema::endpoints::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = endpoints)]
pub struct DbEndpoint {
    pub id: String,
    pub app_id: String,
    pub name: String,
    pub path: String,
    pub sql: String,
    pub primary_keys: String,
    pub created_at: String,
    pub updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize, Clone)]
#[diesel(table_name = endpoints)]
struct NewEndpoint {
    id: String,
    app_id: String,
    name: String,
    path: String,
    primary_keys: String,
    sql: String,
}

#[derive(Queryable, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = source_endpoints)]
struct DBSourceEndpoint {
    source_id: String,
    endpoint_id: String,
    app_id: String,
    created_at: String,
    updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = source_endpoints)]
struct NewSourceEndpoint {
    source_id: String,
    endpoint_id: String,
    app_id: String,
}

impl TryFrom<EndpointInfo> for NewEndpoint {
    type Error = Box<dyn Error>;
    fn try_from(input: EndpointInfo) -> Result<Self, Self::Error> {
        let _generated_id = uuid::Uuid::new_v4().to_string();
        Ok(NewEndpoint {
            id: input.id,
            name: input.name,
            path: input.path,
            sql: input.sql,
            app_id: input.app_id,
            primary_keys: input.primary_keys.join(","),
        })
    }
}
impl TryFrom<DbEndpoint> for EndpointInfo {
    type Error = Box<dyn Error>;

    fn try_from(input: DbEndpoint) -> Result<Self, Self::Error> {
        let primary_keys_arr: Vec<String> = input
            .primary_keys
            .split(',')
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        Ok(EndpointInfo {
            id: input.id,
            app_id: input.app_id,
            name: input.name,
            path: input.path,
            sql: input.sql,
            primary_keys: primary_keys_arr,
        })
    }
}
impl Persistable<'_, EndpointInfo> for EndpointInfo {
    fn save(&mut self, pool: DbPool) -> Result<&mut EndpointInfo, Box<dyn Error>> {
        self.upsert(pool)
    }

    fn by_id(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<EndpointInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DbEndpoint = FilterDsl::filter(
            FilterDsl::filter(endpoints, endpoints::id.eq(input_id)),
            endpoints::app_id.eq(application_id),
        )
        .first(&mut db)?;
        let response = EndpointInfo::try_from(result)?;
        Ok(response)
    }

    fn upsert(&mut self, pool: DbPool) -> Result<&mut EndpointInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
            let _ = apps
                .find(self.app_id.to_owned())
                .first::<Application>(conn)
                .map_err(|err| format!("App_id: {:} {:}", self.app_id.to_owned(), err))?;

            let new_endpoint = NewEndpoint::try_from(self.clone())?;

            insert_into(endpoints)
                .values(&new_endpoint)
                .on_conflict(endpoints::id)
                .do_update()
                .set(&new_endpoint)
                .execute(conn)?;

            self.id = new_endpoint.id;
            Ok(())
        })?;
        Ok(self)
    }

    fn list(
        pool: DbPool,
        application_id: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<EndpointInfo>, Pagination), Box<dyn Error>> {
        let mut db = pool.get()?;
        let offset = offset.unwrap_or(constants::OFFSET);
        let limit = limit.unwrap_or(constants::LIMIT);
        let filter_dsl = FilterDsl::filter(endpoints, endpoints::app_id.eq(application_id));
        let results: Vec<DbEndpoint> = filter_dsl
            .to_owned()
            .offset(offset.into())
            .order_by(endpoints::id.asc())
            .limit(limit.into())
            .load(&mut db)?;
        let total: i64 = filter_dsl.count().get_result(&mut db)?;
        let endpoint_info: Vec<EndpointInfo> = results
            .iter()
            .map(|result| EndpointInfo::try_from(result.clone()).unwrap())
            .collect();
        Ok((
            endpoint_info,
            Pagination {
                limit,
                total: total.try_into().unwrap_or(0),
                offset,
            },
        ))
    }

    fn delete(
        pool: DbPool,
        input_id: String,
        application_id: String,
    ) -> Result<bool, Box<dyn Error>> {
        let mut db = pool.get()?;
        diesel::delete(FilterDsl::filter(
            FilterDsl::filter(endpoints, endpoints::id.eq(input_id)),
            endpoints::app_id.eq(application_id),
        ))
        .execute(&mut db)?;
        Ok(true)
    }
}
