#[cfg(test)]
mod grpc_service {
    use dozer_types::models::connection::{
        Authentication, AuthenticationWrapper, DBType, EventsAuthentication,
    };

    use crate::server::dozer_admin_grpc::{
        ConnectionResponse, CreateConnectionRequest, GetAllConnectionRequest,
        GetAllConnectionResponse, GetTablesRequest, UpdateConnectionRequest,
        ValidateConnectionRequest, ValidateConnectionResponse,
    };
    use crate::services::connection_service::ConnectionService;
    use crate::tests::utils::database_url_for_test_env;
    use crate::tests::utils::establish_test_connection;
    #[test]
    pub fn create_list_update() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);

        let connection_service = ConnectionService::new(db_pool);

        // Create
        let request = CreateConnectionRequest {
            name: "connection_name".to_owned(),
            r#type: DBType::Events as i32,
            authentication: Some(AuthenticationWrapper {
                authentication: Some(Authentication::Events(EventsAuthentication {})),
            }),
        };
        let create_result: ConnectionResponse = connection_service
            .create_connection(request.to_owned())
            .unwrap();
        let created_id = create_result.id.clone();
        assert_eq!(
            create_result.connection.to_owned().unwrap().name,
            request.name
        );
        assert_eq!(
            create_result.connection.to_owned().unwrap().db_type,
            request.r#type
        );
        assert_eq!(
            create_result.connection.unwrap().authentication,
            request.authentication.unwrap().authentication
        );

        // List
        let result: GetAllConnectionResponse = connection_service
            .list(GetAllConnectionRequest {
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();

        assert_eq!(result.connections.last().unwrap().id, created_id);

        let request = UpdateConnectionRequest {
            connection_id: created_id,
            name: "updated_connection_name".to_owned(),
            r#type: 0,
            authentication: None,
        };
        let result: ConnectionResponse = connection_service.update(request.to_owned()).unwrap();
        assert_eq!(result.connection.unwrap().name, request.name);
    }

    #[tokio::test]
    #[ignore]
    pub async fn get_schema_details() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let connection_service = ConnectionService::new(db_pool);
        let request = GetTablesRequest {
            connection_id: "random_id".to_owned(),
        };
        let result = connection_service
            .get_tables(request.to_owned())
            .await
            .unwrap();
        assert!(!result.tables.is_empty());
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
