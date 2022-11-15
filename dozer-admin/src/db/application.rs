use std::error::Error;

use super::{
    constants,
    pool::DbPool,
    schema::{self, apps,sources, connections, endpoints},
    source::DBSource, connection::DbConnection, endpoint::DbEndpoint,
};
use crate::diesel::ExpressionMethods;
use crate::server::dozer_admin_grpc::{ApplicationInfo, Pagination};
use diesel::{*, query_dsl::methods::FilterDsl};
use diesel::{insert_into, AsChangeset, Insertable, QueryDsl, Queryable, RunQueryDsl};
use schema::apps::dsl::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationDetail {
    app: Application,
    sources_connections: Vec<(DBSource, DbConnection)>,
    endpoints: Vec<DbEndpoint>
}
#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[diesel(table_name = apps)]
pub struct Application {
    id: String,
    name: String,
    created_at: String,
    updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = apps)]
struct NewApplication {
    id: String,
    name: String,
}
impl TryFrom<Application> for ApplicationInfo {
    type Error = Box<dyn Error>;
    fn try_from(item: Application) -> Result<Self, Self::Error> {
        Ok(ApplicationInfo {
            id: item.id,
            name: item.name,
            created_at: item.created_at,
            updated_at: item.updated_at,
        })
    }
}

pub struct AppDbService {}
impl AppDbService {
    pub fn new() -> Self {
        Self {}
    }
}
impl Default for AppDbService {
    fn default() -> Self {
        Self::new()
    }
}
impl AppDbService {
    pub fn detail(
        pool: DbPool,
        input_id: String,
    ) -> Result<ApplicationDetail, Box<dyn std::error::Error>> {
        let mut db = pool.get()?;
        let application: Application = apps.find(input_id.to_owned()).first(&mut db)?;
        let sources_connections: Vec<(DBSource, DbConnection)> = FilterDsl::filter(
            sources::table
                .inner_join(connections::table)
                .select((sources::all_columns, connections::all_columns)),
            sources::app_id.eq(input_id.to_owned()),
        ).load::<(DBSource, DbConnection)>(&mut db)?;
        let filter_dsl_endpoints = FilterDsl::filter(endpoints::table, endpoints::app_id.eq(input_id.to_owned()));
        let endpoints: Vec<DbEndpoint> = filter_dsl_endpoints
            .to_owned()
            .order_by(endpoints::id.asc())
            .load(&mut db)?;

        let result = ApplicationDetail {
            app: application,
            sources_connections,
            endpoints,
        };
        Ok(result)
    }
    pub fn save(
        app: ApplicationInfo,
        pool: DbPool,
    ) -> Result<ApplicationInfo, Box<dyn std::error::Error>> {
        let generated_id = uuid::Uuid::new_v4().to_string();
        let new_app = NewApplication {
            id: generated_id.to_owned(),
            name: app.name,
        };
        let mut db = pool.get()?;
        let _inserted = insert_into(apps).values(&new_app).execute(&mut db);
        // query
        let result: Application = apps.find(generated_id).first(&mut db)?;
        let response = ApplicationInfo::try_from(result)?;
        Ok(response)
    }

    pub fn by_id(
        pool: DbPool,
        input_id: String,
    ) -> Result<ApplicationInfo, Box<dyn std::error::Error>> {
        let mut db = pool.get()?;
        let result: Application = apps.find(input_id).first(&mut db)?;
        let response = ApplicationInfo::try_from(result)?;
        Ok(response)
    }

    pub fn list(
        pool: DbPool,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<
        (
            Vec<ApplicationInfo>,
            crate::server::dozer_admin_grpc::Pagination,
        ),
        Box<dyn std::error::Error>,
    > {
        let mut db = pool.get()?;
        let offset = offset.unwrap_or(constants::OFFSET);
        let limit = limit.unwrap_or(constants::LIMIT);
        let results: Vec<Application> = apps
            .offset(offset.into())
            .order_by(apps::created_at.asc())
            .limit(limit.into())
            .load(&mut db)?;
        let total: i64 = apps.count().get_result(&mut db)?;
        let application_info: Vec<ApplicationInfo> = results
            .iter()
            .map(|result| ApplicationInfo::try_from(result.clone()).unwrap())
            .collect();

        Ok((
            application_info,
            Pagination {
                limit,
                total: total.try_into().unwrap(),
                offset,
            },
        ))
    }

    pub fn update(
        pool: DbPool,
        input_id: String,
        update_name: String,
    ) -> Result<ApplicationInfo, Box<dyn std::error::Error>> {
        let mut db = pool.get()?;
        let _ = diesel::update(apps)
            .filter(id.eq(input_id.to_owned()))
            .set(name.eq(update_name))
            .execute(&mut db)?;
        // load back
        let by_id: Application = apps.find(input_id).first(&mut db)?;
        let result = ApplicationInfo::try_from(by_id)?;
        Ok(result)
    }
}
