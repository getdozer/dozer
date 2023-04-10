#[cfg(test)]
mod grpc_service {

    use dozer_orchestrator::cli::generate_connection;
    use dozer_types::models::app_config::Config;

    use crate::services::application_service::AppService;
    use crate::tests::utils::database_url_for_test_env;
    use crate::tests::utils::establish_test_connection;
    use dozer_types::grpc_types::admin::{
        AppResponse, CreateAppRequest, ListAppRequest, ListAppResponse, UpdateAppRequest,
    };

    #[test]
    pub fn list_create_update() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);

        let application_service = AppService::new(db_pool);

        let postgres_config = generate_connection("Postgres");
        let config = Config {
            app_name: "new_app_name".to_owned(),
            home_dir: "dozer".to_owned(),
            cache_dir: "dozer/cache".to_owned(),
            connections: vec![postgres_config],
            ..Default::default()
        };
        let request = CreateAppRequest {
            config: serde_yaml::to_string(&config).unwrap(),
        };
        let create_result: AppResponse = application_service.create(request).unwrap();
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

        assert_eq!(result.apps.len(), 1);

        let mut updated_config = config;
        updated_config.app_name = "updated_app_name".to_owned();
        let request = UpdateAppRequest {
            id: create_result.id,

            config: serde_yaml::to_string(&updated_config).unwrap(),
        };
        let result: AppResponse = application_service.update_app(request).unwrap();
        assert_eq!(result.app.unwrap().app_name, updated_config.app_name);
    }
}
