use dozer_types::models::api_config::ApiConfig;

use crate::{
    db::{persistable::Persistable, pool::DbPool},
    server::dozer_admin_grpc::{
        CreateApiConfigRequest, CreateApiConfigResponse, ErrorResponse, GetApiConfigRequest,
        GetApiConfigResponse, UpdateApiConfigRequest, UpdateApiConfigResponse,
    },
};

use super::converter::default_api_config;

pub struct ApiConfigService {
    db_pool: DbPool,
}
impl ApiConfigService {
    pub fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
    }
}
impl ApiConfigService {
    pub fn create_api_config(
        &self,
        input: CreateApiConfigRequest,
    ) -> Result<CreateApiConfigResponse, ErrorResponse> {
        let mut api_config = default_api_config();
        if input.grpc.is_some() {
            api_config.grpc = input.grpc;
        }
        if input.rest.is_some() {
            api_config.rest = input.rest;
        }
        if input.internal.is_some() {
            api_config.internal = input.internal;
        }
        api_config.app_id = Some(input.app_id);
        api_config.auth = input.auth;
        api_config
            .save(self.db_pool.clone())
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(CreateApiConfigResponse {
            config: Some(api_config),
        })
    }

    pub fn update(
        &self,
        input: UpdateApiConfigRequest,
    ) -> Result<UpdateApiConfigResponse, ErrorResponse> {
        let mut config_by_id = ApiConfig::by_id(self.db_pool.clone(), input.id, input.app_id)
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;

        if input.grpc.is_some() {
            config_by_id.grpc = input.grpc;
        }
        if input.rest.is_some() {
            config_by_id.rest = input.rest;
        }
        if input.internal.is_some() {
            config_by_id.internal = input.internal;
        }
        config_by_id.auth = input.auth;
        config_by_id
            .upsert(self.db_pool.clone())
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(UpdateApiConfigResponse {
            config: Some(config_by_id),
        })
    }
    pub fn get_api_config(
        &self,
        request: GetApiConfigRequest,
    ) -> Result<GetApiConfigResponse, ErrorResponse> {
        let api_config = ApiConfig::by_id(self.db_pool.to_owned(), "".to_owned(), request.app_id)
            .map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        Ok(GetApiConfigResponse {
            config: Some(api_config),
        })
    }
}
