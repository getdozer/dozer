use crate::{
    generator::protoc::utils::get_proto_descriptor,
    grpc::{
        client_server::ApiServer,
        internal_grpc::PipelineResponse,
        typed::{
            tests::{
                fake_internal_pipeline_server::start_fake_internal_grpc_pipeline,
                generated::films::{
                    films_client::FilmsClient, FilmEvent, QueryFilmsRequest, QueryFilmsResponse,
                },
            },
            TypedService,
        },
    },
    CacheEndpoint, PipelineDetails,
};
use dozer_cache::cache::expression::{FilterExpression, QueryExpression};
use dozer_types::{models::api_config::default_api_config, types::Schema};
use futures_util::FutureExt;
use std::{collections::HashMap, env, path::PathBuf, time::Duration};

use super::generated::films::{EventType, FilmEventRequest};
use crate::test_utils;
use tokio::{
    sync::{
        broadcast::{self, Receiver},
        oneshot,
    },
    time::timeout,
};
use tokio_stream::StreamExt;
use tonic::{
    transport::{Endpoint, Server},
    Request,
};

pub fn setup_pipeline() -> (
    HashMap<String, PipelineDetails>,
    HashMap<String, Schema>,
    Receiver<PipelineResponse>,
) {
    let schema_name = String::from("films");
    let (schema, _) = test_utils::get_schema();
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = PipelineDetails {
        schema_name: schema_name.clone(),
        cache_endpoint: CacheEndpoint {
            cache: test_utils::initialize_cache(&schema_name, None),
            endpoint,
        },
    };

    let (tx, rx1) = broadcast::channel::<PipelineResponse>(16);
    let default_api_internal = default_api_config().pipeline_internal.unwrap_or_default();
    ApiServer::setup_broad_cast_channel(tx, default_api_internal).unwrap();
    let mut pipeline_map = HashMap::new();
    pipeline_map.insert("films".to_string(), pipeline_details);

    let mut schema_map = HashMap::new();
    schema_map.insert("films".to_string(), schema);

    (pipeline_map, schema_map, rx1)
}

fn setup_typed_service() -> TypedService {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let path = out_dir
        .join("generated_films.bin")
        .to_string_lossy()
        .to_string();

    let (_, desc) = get_proto_descriptor(path).unwrap();

    let (pipeline_map, schema_map, rx1) = setup_pipeline();

    TypedService::new(desc, pipeline_map, schema_map, rx1)
}

#[tokio::test]
async fn test_grpc_query() {
    let typed_service = setup_typed_service();
    let (_tx, rx) = oneshot::channel::<()>();

    let _jh = tokio::spawn(async move {
        Server::builder()
            .add_service(typed_service)
            .serve_with_shutdown("127.0.0.1:1402".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let channel = Endpoint::from_static("http://127.0.0.1:1402")
        .connect()
        .await
        .unwrap();
    let mut client = FilmsClient::new(channel);
    // create filter expression
    let filter = FilterExpression::Simple(
        "film_id".to_string(),
        dozer_cache::cache::expression::Operator::EQ,
        dozer_types::serde_json::Value::from(524),
    );

    let query = QueryExpression {
        filter: Some(filter),
        ..QueryExpression::default()
    };
    let request = QueryFilmsRequest {
        query: Some(dozer_types::serde_json::to_string(&query).unwrap()),
    };
    let res = client.query(Request::new(request)).await.unwrap();
    let request_response: QueryFilmsResponse = res.into_inner();
    assert!(!request_response.data.len() > 0);
}

#[tokio::test]
async fn test_typed_streaming() {
    let (_sender_shutdown_internal, rx_internal) = oneshot::channel::<()>();
    let default_pipeline_internal = default_api_config().pipeline_internal.unwrap_or_default();
    let _jh1 = tokio::spawn(start_fake_internal_grpc_pipeline(
        default_pipeline_internal.host,
        default_pipeline_internal.port,
        rx_internal,
    ));
    let (_tx, rx) = oneshot::channel::<()>();
    let _jh = tokio::spawn(async move {
        let typed_service = setup_typed_service();
        Server::builder()
            .add_service(typed_service)
            .serve_with_shutdown("127.0.0.1:14032".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(1001)).await;
    let address = "http://127.0.0.1:14032".to_owned();
    let mut client = FilmsClient::connect(address.to_owned()).await.unwrap();

    let request = FilmEventRequest {
        r#type: EventType::All as i32,
        filter: None,
    };
    let stream = client
        .on_event(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let mut stream = stream.take(1);
    while let Some(item) = stream.next().await {
        let response: FilmEvent = item.unwrap();
        assert!(response.new.is_some());
    }
    drop(stream);
    let request = FilmEventRequest {
        r#type: EventType::All as i32,
        filter: Some(r#"{ "film_id": 32 }"#.into()),
    };
    let mut client = FilmsClient::connect(address.to_owned()).await.unwrap();
    let stream = client
        .on_event(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let mut stream = stream.take(1);
    let a = stream.next().await;
    let response = a.unwrap().unwrap();
    assert!(response.new.is_some());
    drop(stream);

    let mut client = FilmsClient::connect(address.to_owned()).await.unwrap();
    let request = FilmEventRequest {
        r#type: EventType::All as i32,
        filter: Some(r#"{ "film_id": 0 }"#.into()),
    };
    let mut stream = client
        .on_event(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    assert!(timeout(Duration::from_secs(1), stream.next())
        .await
        .is_err());
    drop(stream);
}
