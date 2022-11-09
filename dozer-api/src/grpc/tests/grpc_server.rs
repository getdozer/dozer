use super::{
    dozer_test_client::{
        GetFilmsByIdResponse, GetFilmsResponse, OnInsertRequest, QueryFilmsResponse,
    },
    utils::{generate_descriptor, generate_proto},
};
use crate::{
    api_server::PipelineDetails,
    grpc::{
        server::TonicServer,
        tests::{
            dozer_test_client::{
                films_service_client::FilmsServiceClient,
                filter_expression::{self},
                FilterExpression, GetFilmsByIdRequest, GetFilmsRequest, Int32Expression,
                OnInsertResponse, QueryFilmsRequest,
            },
            utils::mock_event_notifier,
        },
    },
    grpc_server::GRPCServer,
    test_utils, CacheEndpoint,
};
use dozer_types::events::Event;
use futures_util::FutureExt;
use heck::ToUpperCamelCase;
use std::{collections::HashMap, time::Duration};
use tempdir::TempDir;
use tokio::sync::{broadcast, oneshot};
use tokio_stream::StreamExt;
use tonic::{
    transport::{Endpoint, Server},
    Request,
};

fn setup_grpc_service(tmp_dir_path: String) -> TonicServer {
    let schema_name = String::from("films");
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = PipelineDetails {
        schema_name: schema_name.to_owned(),
        cache_endpoint: CacheEndpoint {
            cache: test_utils::initialize_cache(&schema_name, None),
            endpoint,
        },
    };
    let schema = test_utils::get_schema();
    let proto_generated_result = generate_proto(
        tmp_dir_path.to_owned(),
        schema_name,
        Some(schema),
    )
    .unwrap();
    let path_to_descriptor = generate_descriptor(tmp_dir_path).unwrap();
    let function_types = proto_generated_result.1;
    let event_notifier = mock_event_notifier();
    let (tx, rx1) = broadcast::channel::<Event>(16);
    GRPCServer::setup_broad_cast_channel(tx, event_notifier).unwrap();
    let mut pipeline_map = HashMap::new();
    pipeline_map.insert(
        format!("Dozer.{}Service", "films".to_upper_camel_case()),
        pipeline_details,
    );
    TonicServer::new(path_to_descriptor, function_types, pipeline_map, rx1)
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
    let channel = Endpoint::from_static("http://127.0.0.1:1400")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsServiceClient::new(channel);
    let request = GetFilmsRequest {};
    let res = client.films(Request::new(request)).await.unwrap();
    let request_response: GetFilmsResponse = res.into_inner();
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
    let channel = Endpoint::from_static("http://127.0.0.1:1401")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsServiceClient::new(channel);
    let request = GetFilmsByIdRequest { film_id: 524 };
    let res = client.by_id(Request::new(request)).await.unwrap();
    let request_response: GetFilmsByIdResponse = res.into_inner();
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
    let channel = Endpoint::from_static("http://127.0.0.1:1402")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsServiceClient::new(channel);
    // create filter expression
    let expression = filter_expression::Expression::FilmId(Int32Expression {
        exp: Some(crate::grpc::tests::dozer_test_client::int32_expression::Exp::Eq(524)),
    });
    let filter_expression = FilterExpression {
        expression: Some(expression),
        and: vec![],
    };
    let request = QueryFilmsRequest {
        limit: Some(50),
        skip: Some(0),
        filter: Some(filter_expression),
        order_by: vec![],
    };
    let res = client.query(Request::new(request)).await.unwrap();
    let request_response: QueryFilmsResponse = res.into_inner();
    assert!(!request_response.film.len() > 0);
}

#[tokio::test]
async fn test_grpc_insert_streaming() {
    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let grpc_service = setup_grpc_service(tmp_dir_path);
    let (_tx, rx) = oneshot::channel::<()>();

    let _jh = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve_with_shutdown("127.0.0.1:1403".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let channel = Endpoint::from_static("http://127.0.0.1:1403")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsServiceClient::new(channel);

    let request = OnInsertRequest {};
    let stream = client
        .on_insert(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let mut stream = stream.take(1);
    while let Some(item) = stream.next().await {
        let response: OnInsertResponse = item.unwrap();
        assert!(response.detail.is_some());
    }
}
