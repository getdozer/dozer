#[cfg(test)]
mod grpc_service {
    use dozer_types::models::connection::{
        Authentication, AuthenticationWrapper, DBType, EventsAuthentication,
    };

    use crate::server::dozer_admin_grpc::{
        CreateConnectionRequest, CreateConnectionResponse, GetAllConnectionRequest,
        GetAllConnectionResponse, GetSchemaRequest, UpdateConnectionRequest,
        UpdateConnectionResponse, ValidateConnectionRequest, ValidateConnectionResponse,
    };
    use crate::services::connection_service::ConnectionService;
    use crate::tests::utils::database_url_for_test_env;
    use crate::tests::utils::{establish_test_connection, get_setup_ids};
    #[test]
    pub fn list() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let connection_service = ConnectionService::new(db_pool);
        let result: GetAllConnectionResponse = connection_service
            .list(GetAllConnectionRequest {
                app_id: setup_ids.app_id,
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();
        assert_eq!(result.connections.len(), setup_ids.connection_ids.len());
        assert!(result.connections[0].id.is_some());
        assert!(setup_ids
            .connection_ids
            .contains(&result.connections[0].id.to_owned().unwrap()));
    }

    #[test]
    pub fn create() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let connection_service = ConnectionService::new(db_pool);
        let request = CreateConnectionRequest {
            app_id: setup_ids.app_id,
            name: "connection_name".to_owned(),
            r#type: DBType::Events as i32,
            authentication: Some(AuthenticationWrapper {
                authentication: Some(Authentication::Events(EventsAuthentication {})),
            }),
        };
        let result: CreateConnectionResponse = connection_service
            .create_connection(request.to_owned())
            .unwrap();
        assert_eq!(result.connection.to_owned().unwrap().name, request.name);
        assert_eq!(
            result.connection.to_owned().unwrap().db_type,
            request.r#type
        );
        assert_eq!(
            result.connection.unwrap().authentication,
            request.authentication.unwrap().authentication
        );
    }
    #[test]
    pub fn update() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let connection_service = ConnectionService::new(db_pool);
        let request = UpdateConnectionRequest {
            app_id: setup_ids.app_id,
            connection_id: setup_ids.connection_ids[0].to_owned(),
            name: "updated_connection_name".to_owned(),
            r#type: 0,
            authentication: None,
        };
        let result: UpdateConnectionResponse =
            connection_service.update(request.to_owned()).unwrap();
        assert_eq!(result.connection.unwrap().name, request.name);
    }
    #[tokio::test]
    pub async fn get_schema_details() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let connection_service = ConnectionService::new(db_pool);
        let request = GetSchemaRequest {
            app_id: setup_ids.app_id,
            connection_id: "random_id".to_owned(),
        };
        let result = connection_service
            .get_schema(request.to_owned())
            .await
            .unwrap();
        assert!(result.tables.len() > 0);
    }

    #[tokio::test]
    #[ignore]
    pub async fn validate_eth_connection() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let connection_service = ConnectionService::new(db_pool);
        let result: ValidateConnectionResponse = connection_service
            .validate_connection(ValidateConnectionRequest {
                name: "test_connection".to_owned(),
                r#type: DBType::Events as i32,
                authentication: Some(AuthenticationWrapper {
                    authentication: Some(Authentication::Events(EventsAuthentication {})),
                }),
            })
            .await
            .unwrap();
        assert!(result.success);
    }
}
