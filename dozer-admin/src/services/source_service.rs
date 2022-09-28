use crate::{
    db::{
        persistable::Persistable,
        pool::{establish_connection, DbPool},
    },
    server::dozer_admin_grpc::{
        self, CreateSourceRequest, CreateSourceResponse, ErrorResponse, GetSourceRequest,
        GetSourceResponse, UpdateSourceRequest, UpdateSourceResponse,
    },
};
pub struct SourceService {
    db_pool: DbPool,
}
impl SourceService {
    pub fn new(database_url: String) -> Self {
        Self {
            db_pool: establish_connection(database_url.clone()),
        }
    }
}
impl SourceService {
    pub fn create_source(
        &self,
        input: CreateSourceRequest,
    ) -> Result<CreateSourceResponse, ErrorResponse> {
        if input.info.is_none() {
            return Err(ErrorResponse {
                message: "Missing source info".to_owned(),
            })?;
        }
        let mut source_info = input.info.unwrap();
        source_info
            .upsert(self.db_pool.to_owned())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        return Ok(CreateSourceResponse {
            info: Some(source_info),
        });
    }
    pub fn get_source(&self, input: GetSourceRequest) -> Result<GetSourceResponse, ErrorResponse> {
        let source_info =
            dozer_admin_grpc::SourceInfo::get_by_id(self.db_pool.to_owned(), input.id).map_err(
                |op| ErrorResponse {
                    message: op.to_string(),
                },
            )?;
        return Ok(GetSourceResponse {
            info: Some(source_info.to_owned()),
            id: source_info.id.unwrap(),
        });
    }

    pub fn update_source(
        &self,
        input: UpdateSourceRequest,
    ) -> Result<UpdateSourceResponse, ErrorResponse> {
        if input.info.is_none() {
            return Err(ErrorResponse {
                message: "Missing source info".to_owned(),
            })?;
        }
        let mut source_info = input.info.unwrap();
        source_info
            .upsert(self.db_pool.to_owned())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        return Ok(UpdateSourceResponse {
            info: Some(source_info.to_owned()),
            id: source_info.id.unwrap(),
        });
    }
}
