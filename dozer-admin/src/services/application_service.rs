use crate::{
    db::{
        application::AppDbService,
        pool::{establish_connection, DbPool},
    },
    server::dozer_admin_grpc::{
        ApplicationInfo, CreateAppRequest, CreateAppResponse, ErrorResponse, ListAppRequest,
        ListAppResponse, Pagination, UpdateAppRequest, UpdateAppResponse,
        StartPipelineResponse,StartPipelineRequest
    },
};

pub struct AppService {
    db_pool: DbPool,
}
impl AppService {
    pub fn new(database_url: String) -> Self {
        Self {
            db_pool: establish_connection(database_url),
        }
    }
}
impl AppService {
    pub fn create(&self, input: CreateAppRequest) -> Result<CreateAppResponse, ErrorResponse> {
        let app_info = ApplicationInfo {
            name: input.app_name,
            ..Default::default()
        };
        let app_info =
            AppDbService::save(app_info, self.db_pool.clone()).map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(CreateAppResponse {
            data: Some(app_info),
        })
    }
    pub fn list(&self, input: ListAppRequest) -> Result<ListAppResponse, ErrorResponse> {
        let data: (Vec<ApplicationInfo>, Pagination) =
            AppDbService::list(self.db_pool.clone(), input.limit, input.offset).map_err(|op| {
                ErrorResponse {
                    message: op.to_string(),
                }
            })?;
        Ok(ListAppResponse {
            data: data.0,
            pagination: Some(data.1),
        })
    }
    pub fn update_app(&self, input: UpdateAppRequest) -> Result<UpdateAppResponse, ErrorResponse> {
        let updated_app: ApplicationInfo =
            AppDbService::update(self.db_pool.clone(), input.app_id, input.name).map_err(|op| {
                ErrorResponse {
                    message: op.to_string(),
                }
            })?;
        Ok(UpdateAppResponse {
            data: Some(updated_app),
        })
    }

    pub fn start_pipeline(&self, input: StartPipelineRequest) -> Result<StartPipelineResponse, ErrorResponse> {
        let app_detail = AppDbService::detail(self.db_pool.clone(), input.app_id).map_err(|op| {
            ErrorResponse {
                message: op.to_string(),
            }
        })?;
        println!("==== app detail {:?}", app_detail);
        Ok(StartPipelineResponse {
            success: true
        })
    }
}
