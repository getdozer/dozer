use super::{
    application::Application,
    constants,
    persistable::Persistable,
    pool::DbPool,
    schema::{self, endpoints, source_endpoints, sources},
};
use crate::db::schema::apps::dsl::apps;
use crate::server::dozer_admin_grpc::{EndpointInfo, Pagination};
use diesel::{delete, insert_into, prelude::*, query_dsl::methods::FilterDsl, ExpressionMethods};
use schema::{endpoints::dsl::*, source_endpoints::dsl::*, sources::dsl::*};
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Queryable, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = endpoints)]
pub struct DbEndpoint {
    id: String,
    app_id: String,
    name: String,
    path: String,
    enable_rest: bool,
    enable_grpc: bool,
    sql: String,
    created_at: String,
    updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize, Clone)]
#[diesel(table_name = endpoints)]
struct NewEndpoint {
    id: String,
    app_id: String,
    name: String,
    path: String,
    enable_rest: bool,
    enable_grpc: bool,
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
        let generated_id = uuid::Uuid::new_v4().to_string();
        Ok(NewEndpoint {
            id: generated_id,
            name: input.name,
            path: input.path,
            enable_rest: input.enable_rest,
            enable_grpc: input.enable_grpc,
            sql: input.sql,
            app_id: input.app_id,
        })
    }
}
impl TryFrom<DbEndpoint> for EndpointInfo {
    type Error = Box<dyn Error>;

    fn try_from(input: DbEndpoint) -> Result<Self, Self::Error> {
        let ids: Vec<String> = Vec::new();
        Ok(EndpointInfo {
            id: input.id,
            app_id: "".to_string(),
            name: input.name,
            path: input.path,
            enable_rest: input.enable_rest,
            enable_grpc: input.enable_grpc,
            sql: input.sql,
            source_ids: ids,
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
        let result: Vec<(String, DbEndpoint)> = FilterDsl::filter(
            FilterDsl::filter(
                source_endpoints::table
                    .inner_join(endpoints::table)
                    .select((source_endpoints::source_id, endpoints::all_columns)),
                endpoints::id.eq(input_id),
            ),
            endpoints::app_id.eq(application_id),
        )
        .load::<(String, DbEndpoint)>(&mut db)?;
        if result.is_empty() {
            return Err("There's no endpoint with input id".to_owned())?;
        }
        let source_ids: Vec<String> = result.iter().map(|element| element.0.to_owned()).collect();
        let db_endpoint = &result[0].1;
        let mut endpoint_info = EndpointInfo::try_from(db_endpoint.to_owned())?;
        endpoint_info.source_ids = source_ids;
        Ok(endpoint_info)
    }

    fn upsert(&mut self, pool: DbPool) -> Result<&mut EndpointInfo, Box<dyn Error>> {
        let mut db = pool.get()?;
        let source_ids = self.source_ids.to_owned();
        // if source_ids.is_empty() {
        //     return Err("Missing source_ids".to_owned())?;
        // }
        let source_ids_len = source_ids.len();
        db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
            let _ = apps
                .find(self.app_id.to_owned())
                .first::<Application>(conn)
                .map_err(|err| format!("App_id: {:}", err))?;
            let source_id_query = FilterDsl::filter(
                FilterDsl::filter(sources, sources::id.eq_any(source_ids.to_owned())),
                sources::app_id.eq(self.app_id.to_owned()),
            )
            .select(sources::id)
            .load::<String>(conn)?;
            if source_id_query.len() != source_ids_len {
                return Err("source ids input is not correct".to_owned())?;
            }
            let new_endpoint = NewEndpoint::try_from(self.clone())?;
            insert_into(endpoints)
                .values(&new_endpoint)
                .on_conflict(endpoints::id)
                .do_update()
                .set(&new_endpoint)
                .execute(conn)?;
            for source_id_value in source_id_query {
                insert_into(source_endpoints)
                    .values(NewSourceEndpoint {
                        source_id: source_id_value.to_owned(),
                        endpoint_id: new_endpoint.clone().id,
                        app_id: self.app_id.to_owned(),
                    })
                    .on_conflict((source_endpoints::endpoint_id, source_endpoints::source_id))
                    .do_nothing()
                    .execute(conn)?;
            }
            delete(FilterDsl::filter(
                FilterDsl::filter(
                    source_endpoints,
                    source_endpoints::endpoint_id.eq(new_endpoint.id.to_owned()),
                ),
                source_endpoints::source_id.ne_all(source_ids.to_owned()),
            ))
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
}
