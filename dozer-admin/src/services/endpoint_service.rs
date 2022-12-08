use crate::{
    db::{persistable::Persistable, pool::DbPool},
    server::dozer_admin_grpc::{
        CreateEndpointRequest, CreateEndpointResponse, DeleteEndpointRequest,
        DeleteEndpointResponse, EndpointInfo, ErrorResponse, GetAllEndpointRequest,
        GetAllEndpointResponse, GetEndpointRequest, GetEndpointResponse, Pagination,
        UpdateEndpointRequest, UpdateEndpointResponse,
    },
};

pub struct EndpointService {
    db_pool: DbPool,
}

impl EndpointService {
    pub fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
    }
}
impl EndpointService {
    pub fn delete(
        &self,
        request: DeleteEndpointRequest,
    ) -> Result<DeleteEndpointResponse, ErrorResponse> {
        let _endpoint_info =
            EndpointInfo::delete(self.db_pool.to_owned(), request.endpoint_id, request.app_id)
                .map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?;
        Ok(DeleteEndpointResponse { success: true })
    }
    pub fn list(
        &self,
        input: GetAllEndpointRequest,
    ) -> Result<GetAllEndpointResponse, ErrorResponse> {
        let endpoints: (Vec<EndpointInfo>, Pagination) = EndpointInfo::list(
            self.db_pool.clone(),
            input.app_id,
            input.limit,
            input.offset,
        )
        .map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        Ok(GetAllEndpointResponse {
            data: endpoints.0,
            pagination: Some(endpoints.1),
        })
    }
    pub fn create_endpoint(
        &self,
        request: CreateEndpointRequest,
    ) -> Result<CreateEndpointResponse, ErrorResponse> {
        let generated_id = uuid::Uuid::new_v4().to_string();
        let mut endpoint_info = EndpointInfo {
            id: generated_id,
            app_id: request.app_id.to_owned(),
            name: request.name.to_owned(),
            path: request.path.to_owned(),
            sql: request.sql,
            primary_keys: request.primary_keys,
        };
        endpoint_info
            .upsert(self.db_pool.to_owned())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(CreateEndpointResponse {
            info: Some(endpoint_info),
        })
    }

    pub fn get_endpoint(
        &self,
        request: GetEndpointRequest,
    ) -> Result<GetEndpointResponse, ErrorResponse> {
        let endpoint_info =
            EndpointInfo::by_id(self.db_pool.to_owned(), request.endpoint_id, request.app_id)
                .map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?;
        Ok(GetEndpointResponse {
            info: Some(endpoint_info),
        })
    }

    pub fn update_endpoint(
        &self,
        request: UpdateEndpointRequest,
    ) -> Result<UpdateEndpointResponse, ErrorResponse> {
        let mut endpoint_by_id =
            EndpointInfo::by_id(self.db_pool.to_owned(), request.id, request.app_id).map_err(
                |err| ErrorResponse {
                    message: err.to_string(),
                },
            )?;
        if let Some(name) = request.name {
            endpoint_by_id.name = name;
        }
        if let Some(path) = request.path {
            endpoint_by_id.path = path;
        }
        if let Some(sql) = request.sql {
            endpoint_by_id.sql = sql;
        }
        endpoint_by_id
            .upsert(self.db_pool.to_owned())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(UpdateEndpointResponse {
            info: Some(endpoint_by_id.to_owned()),
        })
    }
}
