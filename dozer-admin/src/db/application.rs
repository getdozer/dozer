use super::{
    constants,
    persistable::Persistable,
    pool::DbPool,
    schema::{self, apps},
};
use crate::diesel::ExpressionMethods;
use crate::server::dozer_admin_grpc::{ApplicationInfo, Pagination};
use diesel::{
    insert_into, AsChangeset, Identifiable, Insertable, QueryDsl, Queryable, RunQueryDsl,
};
use dozer_types::models::app_config;
use schema::apps::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;
#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = apps)]
pub struct Application {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
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
    pub fn save(
        app: ApplicationInfo,
        pool: DbPool,
    ) -> Result<ApplicationInfo, Box<dyn std::error::Error>> {
        let new_app = NewApplication {
            id: app.to_owned().id,
            name: app.to_owned().name,
        };
        let mut db = pool.get()?;
        let _inserted = insert_into(apps).values(&new_app).execute(&mut db);
        // query
        let result: Application = apps.find(app.to_owned().id).first(&mut db)?;
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

impl Persistable<'_, app_config::Config> for app_config::Config {
    fn save(&mut self, pool: DbPool) -> Result<&mut app_config::Config, Box<dyn Error>> {
        // save App
        let generated_app_id = uuid::Uuid::new_v4().to_string();
        let app_id = self.id.to_owned().unwrap_or(generated_app_id);
        if self.app_name.is_empty() {
            self.app_name = "build_your_first_app".to_owned()
        }
        let app_info = ApplicationInfo {
            id: app_id.to_owned(),
            name: self.app_name.to_owned(),
            ..Default::default()
        };
        AppDbService::save(app_info, pool.clone())?;
        self.id = Some(app_id.to_owned());

        let mut api_config = self.api.to_owned().unwrap_or_default();
        api_config.app_id = Some(app_id.to_owned());
        if api_config.id.is_none() {
            api_config.id = Some(uuid::Uuid::new_v4().to_string())
        }
        api_config.save(pool.clone())?;
        self.api = Some(api_config);

        // connection
        let connections = self
            .connections
            .to_owned()
            .into_iter()
            .map(|mut con| {
                con.app_id = Some(app_id.to_owned());
                if con.id.is_none() {
                    con.id = Some(uuid::Uuid::new_v4().to_string())
                }
                let connection = con.save(pool.to_owned()).unwrap();
                connection.to_owned()
            })
            .collect();
        self.connections = connections;
        // sources
        let sources = self
            .sources
            .to_owned()
            .into_iter()
            .map(|mut src| {
                src.app_id = Some(app_id.to_owned());
                if src.id.is_none() {
                    src.id = Some(uuid::Uuid::new_v4().to_string())
                }
                let source = src.save(pool.to_owned()).unwrap();
                source.to_owned()
            })
            .collect();
        self.sources = sources;
        // endpoints
        let endpoints = self
            .endpoints
            .to_owned()
            .into_iter()
            .map(|mut ep| {
                ep.app_id = Some(app_id.to_owned());
                if ep.id.is_none() {
                    ep.id = Some(uuid::Uuid::new_v4().to_string())
                }
                let endpoint = ep.save(pool.to_owned()).unwrap();
                endpoint.to_owned()
            })
            .collect();
        self.endpoints = endpoints;
        Ok(self)
    }

    fn by_id(
        _pool: DbPool,
        _input_id: String,
        _app_id: String,
    ) -> Result<app_config::Config, Box<dyn Error>> {
        todo!()
    }

    fn list(
        _pool: DbPool,
        _app_id: String,
        _limit: Option<u32>,
        _offset: Option<u32>,
    ) -> Result<(Vec<app_config::Config>, Pagination), Box<dyn Error>> {
        todo!()
    }

    fn upsert(&mut self, _pool: DbPool) -> Result<&mut app_config::Config, Box<dyn Error>> {
        todo!()
    }

    fn delete(_pool: DbPool, _input_id: String, _app_id: String) -> Result<bool, Box<dyn Error>> {
        todo!()
    }
}
