use super::{
    application::Application,
    constants,
    persistable::Persistable,
    schema::{
        self,
        configs::{self, app_id},
    },
};
use crate::db::schema::apps::dsl::apps;
use crate::diesel::ExpressionMethods;
use crate::server::dozer_admin_grpc::Pagination;
use diesel::{insert_into, AsChangeset, Insertable, QueryDsl, Queryable, RunQueryDsl};
use diesel::{query_dsl::methods::FilterDsl, *};
use dozer_types::models::{
    api_config::{
        default_api_config, ApiConfig, ApiGrpc, ApiInternal, ApiPipelineInternal, ApiRest,
    },
    api_security::ApiSecurity,
};
use schema::configs::dsl::*;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Identifiable, Queryable, PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
#[diesel(table_name = configs)]
pub struct DBApiConfig {
    pub(crate) id: String,
    pub(crate) app_id: String,
    pub(crate) api_security: Option<String>,
    pub(crate) rest: Option<String>,
    pub(crate) grpc: Option<String>,
    pub(crate) auth: Option<bool>,
    pub(crate) api_internal: Option<String>,
    pub(crate) pipeline_internal: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}
#[derive(Insertable, AsChangeset, PartialEq, Debug, Serialize, Deserialize)]
#[diesel(table_name = configs)]
struct NewApiConfig {
    pub(crate) id: String,
    pub(crate) app_id: String,
    pub(crate) api_security: Option<String>,
    pub(crate) rest: Option<String>,
    pub(crate) grpc: Option<String>,
    pub(crate) api_internal: Option<String>,
    pub(crate) pipeline_internal: Option<String>,
    pub(crate) auth: Option<bool>,
}
impl TryFrom<DBApiConfig> for ApiConfig {
    type Error = Box<dyn Error>;
    fn try_from(item: DBApiConfig) -> Result<Self, Self::Error> {
        let default_api_config = default_api_config();
        let rest_value = item.rest.map_or_else(
            || default_api_config.rest,
            |str| serde_json::from_str::<ApiRest>(&str).ok(),
        );
        let grpc_value = item.grpc.map_or_else(
            || default_api_config.grpc,
            |str| serde_json::from_str::<ApiGrpc>(&str).ok(),
        );
        let internal_api = item.api_internal.map_or_else(
            || default_api_config.api_internal,
            |str| serde_json::from_str::<ApiInternal>(&str).ok(),
        );
        let internal_pipeline = item.pipeline_internal.map_or_else(
            || default_api_config.pipeline_internal,
            |str| serde_json::from_str::<ApiPipelineInternal>(&str).ok(),
        );
        let security_jwt = item.api_security.map_or_else(
            || default_api_config.api_security,
            |str| serde_json::from_str::<ApiSecurity>(&str).ok(),
        );
        Ok(ApiConfig {
            rest: rest_value,
            grpc: grpc_value,
            api_security: security_jwt,
            api_internal: internal_api,
            pipeline_internal: internal_pipeline,
            auth: item.auth.unwrap_or_default(),
            app_id: Some(item.app_id),
            id: Some(item.id),
        })
    }
}

impl TryFrom<ApiConfig> for NewApiConfig {
    type Error = Box<dyn Error>;
    fn try_from(item: ApiConfig) -> Result<Self, Self::Error> {
        let rest_value = item.rest.map_or_else(
            || None,
            |rest_config| serde_json::to_string(&rest_config).ok(),
        );
        let grpc_value = item.grpc.map_or_else(
            || None,
            |grpc_config| serde_json::to_string(&grpc_config).ok(),
        );
        let internal_api = item.api_internal.map_or_else(
            || None,
            |api_internal_config| serde_json::to_string(&api_internal_config).ok(),
        );
        let internal_pipeline = item.pipeline_internal.map_or_else(
            || None,
            |internal_pipeline_config| serde_json::to_string(&internal_pipeline_config).ok(),
        );
        let api_security_config = item.api_security.map_or_else(
            || None,
            |iapi_security_config| serde_json::to_string(&iapi_security_config).ok(),
        );

        let generated_id = uuid::Uuid::new_v4().to_string();
        let id_str = item.id.unwrap_or(generated_id);
        Ok(NewApiConfig {
            id: id_str,
            app_id: item.app_id.unwrap(),
            rest: rest_value,
            grpc: grpc_value,
            api_internal: internal_api,
            pipeline_internal: internal_pipeline,
            auth: Some(item.auth),
            api_security: api_security_config,
        })
    }
}
impl Persistable<'_, ApiConfig> for ApiConfig {
    fn save(&mut self, pool: super::pool::DbPool) -> Result<&mut ApiConfig, Box<dyn Error>> {
        self.upsert(pool)
    }

    fn by_id(
        pool: super::pool::DbPool,
        _input_id: String,
        application_id: String,
    ) -> Result<ApiConfig, Box<dyn Error>> {
        let mut db = pool.get()?;
        let result: DBApiConfig =
            FilterDsl::filter(configs, app_id.eq(application_id)).first(&mut db)?;
        let result = ApiConfig::try_from(result)?;
        Ok(result)
    }

    fn list(
        pool: super::pool::DbPool,
        application_id: String,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<(Vec<ApiConfig>, Pagination), Box<dyn Error>> {
        let mut db = pool.get()?;
        let offset = offset.unwrap_or(constants::OFFSET);
        let limit = limit.unwrap_or(constants::LIMIT);
        let filter_dsl = FilterDsl::filter(configs, app_id.eq(application_id));
        let results: Vec<DBApiConfig> = filter_dsl
            .to_owned()
            .offset(offset.into())
            .order_by(configs::id.asc())
            .limit(limit.into())
            .load(&mut db)?;
        let total: i64 = filter_dsl.count().get_result(&mut db)?;
        let config_lst: Vec<ApiConfig> = results
            .iter()
            .map(|result| ApiConfig::try_from(result.clone()).unwrap())
            .collect();
        Ok((
            config_lst,
            Pagination {
                limit,
                total: total.try_into().unwrap_or(0),
                offset,
            },
        ))
    }

    fn upsert(&mut self, pool: super::pool::DbPool) -> Result<&mut ApiConfig, Box<dyn Error>> {
        let new_config = NewApiConfig::try_from(self.to_owned())?;
        let mut db = pool.get()?;
        db.transaction::<(), _, _>(|conn| -> Result<(), Box<dyn Error>> {
            let _ = apps
                .find(new_config.app_id.to_owned())
                .first::<Application>(conn)
                .map_err(|err| format!("App_id: {err:}"))?;
            let _inserted = insert_into(configs)
                .values(&new_config)
                .on_conflict(configs::id)
                .do_update()
                .set(&new_config)
                .execute(conn);
            self.id = Some(new_config.id);
            Ok(())
        })?;

        Ok(self)
    }

    fn delete(
        _pool: super::pool::DbPool,
        _input_id: String,
        _application_id: String,
    ) -> Result<bool, Box<dyn Error>> {
        todo!()
    }
}
