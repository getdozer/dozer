#[cfg(test)]
mod grpc_service {
    use crate::server::dozer_admin_grpc::{
        create_source_request, CreateSourceRequest, CreateSourceResponse, GetAllSourceRequest,
        GetAllSourceResponse, GetSourceRequest, GetSourceResponse, UpdateSourceRequest,
        UpdateSourceResponse,
    };
    use crate::services::source_service::SourceService;
    use crate::tests::util_sqlite_setup::database_url_for_test_env;
    use crate::tests::util_sqlite_setup::{establish_test_connection, get_setup_ids};
    #[test]
    pub fn list() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let source_service = SourceService::new(db_pool);
        let result: GetAllSourceResponse = source_service
            .list(GetAllSourceRequest {
                app_id: setup_ids.app_id,
                limit: Some(100),
                offset: Some(0),
            })
            .unwrap();
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].id, setup_ids.source_ids[0]);
    }
    #[test]
    pub fn get() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let source_service = SourceService::new(db_pool);
        let result: GetSourceResponse = source_service
            .get_source(GetSourceRequest {
                app_id: setup_ids.app_id,
                id: setup_ids.source_ids[0].to_owned(),
            })
            .unwrap();
        assert_eq!(result.info.unwrap().id, setup_ids.source_ids[0]);
    }

    #[test]
    pub fn update() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let source_service = SourceService::new(db_pool);
        let result: UpdateSourceResponse = source_service
            .update_source(UpdateSourceRequest {
                app_id: setup_ids.app_id,
                id: setup_ids.source_ids[0].to_owned(),
                name: Some("update_source_name".to_owned()),
                connection: None,
                table_name: None,
            })
            .unwrap();
        assert_eq!(result.info.unwrap().id, setup_ids.source_ids[0]);
    }

    #[test]
    pub fn create() {
        let test_db_connection = database_url_for_test_env();
        let db_pool = establish_test_connection(test_db_connection);
        let setup_ids = get_setup_ids();
        let source_service = SourceService::new(db_pool);
        let request = CreateSourceRequest {
            app_id: setup_ids.app_id,
            name: "new_source_name".to_owned(),
            table_name: "source_table_name".to_owned(),
            connection: Some(create_source_request::Connection::ConnectionId(
                setup_ids.connection_ids[0].to_owned(),
            )),
        };
        let result: CreateSourceResponse =
            source_service.create_source(request.to_owned()).unwrap();
        assert_eq!(result.info.unwrap().name, request.name);
    }
}
