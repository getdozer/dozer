use super::utils::{generate_descriptor, generate_proto};
use crate::{
    api_server::PipelineDetails, generator::protoc::proto_service::GrpcType,
    grpc::server::TonicServer, test_utils,
};
use futures_util::FutureExt;
use std::{collections::HashMap, time::Duration};
use tempdir::TempDir;
use tokio::sync::oneshot;
use tonic::{
    transport::{Endpoint, Server},
    Request,
};

fn setup(tmp_dir_path: String, schema_name: String) -> (String, HashMap<String, GrpcType>) {
    let schema = test_utils::get_schema();
    let proto_generated_result =
        generate_proto(tmp_dir_path.to_owned(), schema_name.to_owned(), schema).unwrap();
    let path_to_descriptor = generate_descriptor(tmp_dir_path, schema_name).unwrap();
    (path_to_descriptor, proto_generated_result.1)
}

#[tokio::test]
async fn test_grpc_list() {
    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let schema_name = String::from("film");
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = PipelineDetails {
        schema_name: schema_name.to_owned(),
        endpoint,
    };
    let cache = test_utils::initialize_cache(&schema_name.to_owned());
    let setup_result = setup(tmp_dir_path, schema_name.to_owned());
    let path_to_descriptor = setup_result.0;
    let function_types = setup_result.1;
    let grpc_service =
        TonicServer::new(path_to_descriptor, function_types, cache, pipeline_details);
    let (_tx, rx) = oneshot::channel::<()>();

    let _jh = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve_with_shutdown("127.0.0.1:1400".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    pub mod dozer_client_generated {
        include!("dozer-test-client.rs");
    }
    use dozer_client_generated::films_client::FilmsClient;
    let channel = Endpoint::from_static("http://127.0.0.1:1400")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsClient::new(channel);
    let request = dozer_client_generated::GetFilmsRequest {};
    let res = client.films(Request::new(request)).await.unwrap();
    let request_response: dozer_client_generated::GetFilmsResponse = res.into_inner();
    assert!(!request_response.film.is_empty());
}

#[tokio::test]
async fn test_grpc_get_by_id() {
    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let schema_name = String::from("film");
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = PipelineDetails {
        schema_name: schema_name.to_owned(),
        endpoint,
    };
    let cache = test_utils::initialize_cache(&pipeline_details.schema_name.to_owned());
    let setup_result = setup(tmp_dir_path, schema_name.to_owned());
    let path_to_descriptor = setup_result.0;
    let function_types = setup_result.1;
    let grpc_service =
        TonicServer::new(path_to_descriptor, function_types, cache, pipeline_details);
    let (_tx, rx) = oneshot::channel::<()>();

    let _jh = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve_with_shutdown("127.0.0.1:1401".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    pub mod dozer_client_generated {
        include!("dozer-test-client.rs");
    }
    use dozer_client_generated::films_client::FilmsClient;
    let channel = Endpoint::from_static("http://127.0.0.1:1401")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsClient::new(channel);
    let request = dozer_client_generated::GetFilmsByIdRequest { film_id: 524 };
    let res = client.by_id(Request::new(request)).await.unwrap();
    let request_response: dozer_client_generated::GetFilmsByIdResponse = res.into_inner();
    assert!(request_response.film.is_some());
}

#[tokio::test]
async fn test_grpc_query() {
    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let schema_name = String::from("film");
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = PipelineDetails {
        schema_name,
        endpoint,
    };
    let cache = test_utils::initialize_cache(&pipeline_details.schema_name.to_owned());
    let setup_result = setup(tmp_dir_path, pipeline_details.schema_name.to_owned());
    let path_to_descriptor = setup_result.0;
    let function_types = setup_result.1;
    let grpc_service =
        TonicServer::new(path_to_descriptor, function_types, cache, pipeline_details);
    let (_tx, rx) = oneshot::channel::<()>();

    let _jh = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve_with_shutdown("127.0.0.1:1402".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    pub mod dozer_client_generated {
        include!("dozer-test-client.rs");
    }
    use dozer_client_generated::films_client::FilmsClient;
    let channel = Endpoint::from_static("http://127.0.0.1:1402")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsClient::new(channel);
    let request = dozer_client_generated::QueryFilmsRequest {
        limit: Some(50),
        skip: Some(0),
        filter: None,
        order_by: vec![],
    };
    let res = client.query(Request::new(request)).await.unwrap();
    let request_response: dozer_client_generated::QueryFilmsResponse = res.into_inner();
    assert!(!request_response.film.is_empty());
}
