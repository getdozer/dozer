#[cfg(test)]
mod grpc_service {
    use crate::server::dozer_admin_grpc::{
        authentication, Authentication, CreateConnectionRequest, CreateConnectionResponse,
        EthereumAuthentication, GetAllConnectionRequest, GetAllConnectionResponse,
        ValidateConnectionRequest, ValidateConnectionResponse,
    };
    use crate::services::connection_service::ConnectionService;
    use crate::tests::util_sqlite_setup::database_url_for_test_env;
    use crate::tests::util_sqlite_setup::{establish_test_connection, get_setup_ids};
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
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].id, setup_ids.connection_ids[0]);
    }

    #[test]
    #[ignore]
    pub fn create() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let connection_service = ConnectionService::new(db_pool);
        let request = CreateConnectionRequest {
            app_id: setup_ids.app_id,
            name: "connection_name".to_owned(),
            r#type: 0,
            authentication: Some(Authentication {
                authentication: Some(authentication::Authentication::Ethereum(
                    EthereumAuthentication {
                        wss_url: "wss::link".to_owned(),
                        filter: None,
                    },
                )),
            }),
        };
        let result: CreateConnectionResponse = connection_service
            .create_connection(request.to_owned())
            .unwrap();
        assert_eq!(result.data.unwrap().name, request.name);
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
                r#type: 3,
                authentication: Some(Authentication {
                    authentication: Some(authentication::Authentication::Ethereum(
                        EthereumAuthentication {
                            wss_url: "wss::link".to_owned(),
                            filter: None,
                        },
                    )),
                }),
            })
            .await
            .unwrap();
        assert!(result.success);
    }
}
