use crate::{
    db::{
        persistable::Persistable,
        pool::{establish_connection, DbPool},
    },
    server::dozer_admin_grpc::{
        CreateEndpointRequest, CreateEndpointResponse, ErrorResponse, GetEndpointRequest,
        GetEndpointResponse, UpdateEndpointRequest, UpdateEndpointResponse, EndpointInfo,
    },
};

pub struct EndpointService {
    db_pool: DbPool,
}

impl EndpointService {
    pub fn new(database_url: String) -> Self {
        Self {
            db_pool: establish_connection(database_url),
        }
    }
}
impl EndpointService {
   pub fn create_endpoint(
        &self,
        request: CreateEndpointRequest,
    ) -> Result<CreateEndpointResponse, ErrorResponse> {
        if request.info.is_none() {
            return Err(ErrorResponse {
                message: "Missing endpoint info".to_owned(),
            })?;
        }
        let mut endpoint_info = request.info.unwrap();
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
            EndpointInfo::get_by_id(self.db_pool.to_owned(), request.endpoint_id).map_err(
                |op| ErrorResponse {
                    message: op.to_string(),
                },
            )?;
        Ok(GetEndpointResponse {
            info: Some(endpoint_info),
        })
    }

    pub fn update_endpoint(
        &self,
        request: UpdateEndpointRequest,
    ) -> Result<UpdateEndpointResponse, ErrorResponse> {
        if request.info.is_none() {
            return Err(ErrorResponse {
                message: "Missing endpoint info".to_owned(),
            })?;
        }
        let mut endpoint_info = request.info.unwrap();
        endpoint_info
            .upsert(self.db_pool.to_owned())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(UpdateEndpointResponse {
            info: Some(endpoint_info.to_owned()),
        })
    }
}
