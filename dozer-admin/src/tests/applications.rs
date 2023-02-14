#[cfg(test)]
mod grpc_service {
    use dozer_orchestrator::cli::generate_connection;
    use dozer_types::models::app_config::Config;

    use crate::server::dozer_admin_grpc::{
        CreateAppRequest, CreateAppResponse, ListAppRequest, ListAppResponse, StartPipelineRequest,
        StartPipelineResponse, UpdateAppRequest, UpdateAppResponse,
    };
    use crate::services::application_service::AppService;
    use crate::tests::utils::database_url_for_test_env;
    use crate::tests::utils::{establish_test_connection, get_setup_ids};
    #[test]
    pub fn list_create_update() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let application_service = AppService::new(db_pool, "dozer".to_owned());

        let config = generate_connection("Postgres");
        let config = Config {
            connections: vec![config],
            ..Default::default()
        };
        let request = CreateAppRequest {
            app_name: "new_app_name".to_owned(),
            config: serde_yaml::to_string(&config).unwrap(),
        };
        let create_result: CreateAppResponse =
            application_service.create(request.to_owned()).unwrap();
        assert_eq!(create_result.app.as_ref().unwrap().name, request.app_name);

        let result: ListAppResponse = application_service
            .list(ListAppRequest {
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();
        assert!(!result.apps.is_empty());
        assert_eq!(result.apps[0].id, setup_ids.app_id);

        let request = UpdateAppRequest {
            app_id: create_result.app.as_ref().unwrap().id.clone(),
            name: "updated_app_name".to_owned(),
        };
        let result: UpdateAppResponse = application_service.update_app(request.to_owned()).unwrap();
        assert_eq!(result.app.unwrap().name, request.name);
    }

    #[test]
    #[ignore]
    pub fn trigger_cli() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let application_service = AppService::new(db_pool, "dozer".to_owned());
        let setup_ids = get_setup_ids();

        let request = StartPipelineRequest {
            app_id: setup_ids.app_id,
        };
        let result: StartPipelineResponse = application_service.start_pipeline(request).unwrap();
        assert!(result.success);
    }
}
