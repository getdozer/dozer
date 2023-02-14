#[cfg(test)]
mod grpc_service {
    use dozer_orchestrator::cli::generate_connection;
    use dozer_types::models::app_config::Config;

    use crate::server::dozer_admin_grpc::{
        AppResponse, CreateAppRequest, ListAppRequest, ListAppResponse, StartPipelineRequest,
        StartPipelineResponse, UpdateAppRequest,
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
            app_name: "new_app_name".to_owned(),
            connections: vec![config],
            ..Default::default()
        };
        let request = CreateAppRequest {
            config: serde_yaml::to_string(&config).unwrap(),
        };
        let create_result: AppResponse = application_service.create(request.to_owned()).unwrap();
        assert_eq!(
            create_result.app.as_ref().unwrap().app_name,
            config.app_name
        );

        let result: ListAppResponse = application_service
            .list(ListAppRequest {
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();
        assert!(!result.apps.is_empty());
        assert_eq!(result.apps[0].id, setup_ids.app_id);

        let mut updated_config = config.clone();
        updated_config.app_name = "updated_app_name".to_owned();
        let request = UpdateAppRequest {
            id: create_result.id.clone(),

            config: serde_yaml::to_string(&updated_config).unwrap(),
        };
        let result: AppResponse = application_service.update_app(request.to_owned()).unwrap();
        assert_eq!(result.app.unwrap().app_name, updated_config.app_name);
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
