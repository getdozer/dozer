#[cfg(test)]
mod grpc_service {
    use crate::server::dozer_admin_grpc::{
        GetAllConnectionRequest,GetAllConnectionResponse
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
        let result: GetAllConnectionResponse  = connection_service
            .list(GetAllConnectionRequest {
                app_id: setup_ids.app_id,
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].id, setup_ids.connection_ids[0]);
    }
}
