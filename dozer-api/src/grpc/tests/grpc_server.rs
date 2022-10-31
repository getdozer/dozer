use super::utils::{generate_descriptor, generate_proto};
use crate::{
    api_server::PipelineDetails,
    grpc::{server::TonicServer, tests::utils::mock_event_notifier},
    grpc_server::GRPCServer,
    test_utils,
};
use dozer_types::events::Event;
use futures_util::FutureExt;
use std::time::Duration;
use tempdir::TempDir;
use tokio::sync::{broadcast, oneshot};
use tonic::{
    transport::{Endpoint, Server},
    Request,
};

fn setup_grpc_service(tmp_dir_path: String) -> TonicServer {
    let schema_name = String::from("film");
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = PipelineDetails {
        schema_name: schema_name.to_owned(),
        endpoint,
    };
    let cache = test_utils::initialize_cache(&schema_name);
    let proto_generated_result =
        generate_proto(tmp_dir_path.to_owned(), schema_name.to_owned()).unwrap();
    let path_to_descriptor = generate_descriptor(tmp_dir_path, schema_name).unwrap();
    let function_types = proto_generated_result.1;
    let event_notifier = mock_event_notifier();
    let (tx, rx1) = broadcast::channel::<Event>(16);
    GRPCServer::setup_broad_cast_channel(tx, event_notifier).unwrap();
    
    TonicServer::new(
        path_to_descriptor,
        function_types,
        cache,
        pipeline_details,
        rx1,
    )
}

#[tokio::test]
async fn test_grpc_list() {
    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let grpc_service = setup_grpc_service(tmp_dir_path);
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
    use dozer_client_generated::films_service_client::FilmsServiceClient;
    let channel = Endpoint::from_static("http://127.0.0.1:1400")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsServiceClient::new(channel);
    let request = dozer_client_generated::GetFilmsRequest {};
    let res = client.films(Request::new(request)).await.unwrap();
    let request_response: dozer_client_generated::GetFilmsResponse = res.into_inner();
    assert!(!request_response.film.is_empty());
}

#[tokio::test]
async fn test_grpc_get_by_id() {
    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let grpc_service = setup_grpc_service(tmp_dir_path);
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
    use dozer_client_generated::films_service_client::FilmsServiceClient;
    let channel = Endpoint::from_static("http://127.0.0.1:1401")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsServiceClient::new(channel);
    let request = dozer_client_generated::GetFilmsByIdRequest { film_id: 524 };
    let res = client.by_id(Request::new(request)).await.unwrap();
    let request_response: dozer_client_generated::GetFilmsByIdResponse = res.into_inner();
    assert!(request_response.film.is_some());
}

#[tokio::test]
async fn test_grpc_query() {
    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let grpc_service = setup_grpc_service(tmp_dir_path);
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
    use dozer_client_generated::films_service_client::FilmsServiceClient;
    let channel = Endpoint::from_static("http://127.0.0.1:1402")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsServiceClient::new(channel);
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
