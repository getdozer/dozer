#[cfg(test)]
mod grpc_service {
    use crate::server::dozer_admin_grpc::{
        CreateAppRequest, CreateAppResponse, ListAppRequest, ListAppResponse, StartPipelineRequest,
        StartPipelineResponse, UpdateAppRequest, UpdateAppResponse,
    };
    use crate::services::application_service::AppService;
    use crate::tests::util_sqlite_setup::database_url_for_test_env;
    use crate::tests::util_sqlite_setup::{establish_test_connection, get_setup_ids};
    #[test]
    pub fn list() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let application_service = AppService::new(db_pool);
        let result: ListAppResponse = application_service
            .list(ListAppRequest {
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();
        assert!(!result.data.is_empty());
        assert_eq!(result.data[0].id, setup_ids.app_id);
    }

    #[test]
    pub fn create() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let application_service = AppService::new(db_pool);
        let request = CreateAppRequest {
            app_name: "new_app_name".to_owned(),
        };
        let result: CreateAppResponse = application_service.create(request.to_owned()).unwrap();
        assert_eq!(result.data.unwrap().name, request.app_name);
    }

    #[test]
    pub fn update() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let application_service = AppService::new(db_pool);
        let setup_ids = get_setup_ids();

        let request = UpdateAppRequest {
            app_id: setup_ids.app_id,
            name: "updated_app_name".to_owned(),
        };
        let result: UpdateAppResponse = application_service.update_app(request.to_owned()).unwrap();
        assert_eq!(result.data.unwrap().name, request.name);
    }

    #[test]
    #[ignore]
    pub fn trigger_cli() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let application_service = AppService::new(db_pool);
        let setup_ids = get_setup_ids();

        let request = StartPipelineRequest {
            app_id: setup_ids.app_id,
        };
        let result: StartPipelineResponse = application_service.start_pipeline(request).unwrap();
        assert!(result.success);
    }
}
