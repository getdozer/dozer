#[cfg(test)]
mod grpc_service {
    use crate::server::dozer_admin_grpc::{
        CreateEndpointRequest, CreateEndpointResponse, DeleteEndpointRequest,
        DeleteEndpointResponse, GetAllEndpointRequest, GetAllEndpointResponse, GetEndpointRequest,
        GetEndpointResponse, UpdateEndpointRequest, UpdateEndpointResponse,
    };
    use crate::services::endpoint_service::EndpointService;
    use crate::tests::util_sqlite_setup::database_url_for_test_env;
    use crate::tests::util_sqlite_setup::{establish_test_connection, get_setup_ids};
    #[test]
    pub fn list() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let endpoint_service = EndpointService::new(db_pool);
        let result: GetAllEndpointResponse = endpoint_service
            .list(GetAllEndpointRequest {
                app_id: setup_ids.app_id,
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();
        assert!(!result.data.is_empty());
        assert_eq!(result.data[0].id, setup_ids.api_ids[0]);
    }

    #[test]
    pub fn get() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let endpoint_service = EndpointService::new(db_pool);
        let result: GetEndpointResponse = endpoint_service
            .get_endpoint(GetEndpointRequest {
                app_id: setup_ids.app_id,
                endpoint_id: setup_ids.api_ids[0].to_owned(),
            })
            .unwrap();
        assert_eq!(result.info.unwrap().id, setup_ids.api_ids[0].to_owned());
    }

    #[test]
    pub fn create() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let endpoint_service = EndpointService::new(db_pool);
        let setup_ids = get_setup_ids();
        let request = CreateEndpointRequest {
            app_id: setup_ids.app_id,
            name: "new_endpoint".to_owned(),
            path: "/new_endpoint".to_owned(),
            enable_rest: true,
            enable_grpc: true,
            sql: "select block_number, id  from eth_logs where 1=1 group by block_number, id;"
                .to_owned(),
            source_ids: vec![],
            primary_keys: vec!["id".to_owned()],
        };
        let result: CreateEndpointResponse = endpoint_service
            .create_endpoint(request.to_owned())
            .unwrap();
        assert_eq!(result.info.unwrap().name, request.name);
    }

    #[test]
    pub fn update() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let endpoint_service = EndpointService::new(db_pool);
        let setup_ids = get_setup_ids();

        let request = UpdateEndpointRequest {
            app_id: setup_ids.app_id,
            id: setup_ids.api_ids[0].to_owned(),
            name: Some("updated_endpoint_name".to_owned()),
            enable_grpc: None,
            enable_rest: None,
            path: None,
            primary_keys: vec![],
            source_ids: vec![],
            sql: None,
        };
        let result: UpdateEndpointResponse = endpoint_service
            .update_endpoint(request.to_owned())
            .unwrap();
        assert_eq!(result.info.unwrap().name, request.name.unwrap());
    }

    #[test]
    pub fn update_endpoint_not_exist() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let endpoint_service = EndpointService::new(db_pool);
        let setup_ids = get_setup_ids();

        let request = UpdateEndpointRequest {
            app_id: setup_ids.app_id,
            id: "random_id".to_owned(),
            name: Some("updated_endpoint_name".to_owned()),
            enable_grpc: None,
            enable_rest: None,
            path: None,
            primary_keys: vec![],
            source_ids: vec![],
            sql: None,
        };
        let result = endpoint_service.update_endpoint(request);
        assert!(result.is_err());
    }

    #[test]
    pub fn delete() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let endpoint_service = EndpointService::new(db_pool);
        let setup_ids = get_setup_ids();

        let request = DeleteEndpointRequest {
            app_id: setup_ids.app_id,
            endpoint_id: setup_ids.api_ids[0].to_owned(),
        };
        let result: DeleteEndpointResponse = endpoint_service.delete(request).unwrap();
        assert!(result.success);
    }
}
