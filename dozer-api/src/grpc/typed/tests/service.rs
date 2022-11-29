use crate::{
    grpc::{
        client_server::ApiServer,
        internal_grpc::PipelineRequest,
        typed::{
            tests::generated::films::{
                films_client::FilmsClient, FilmEvent, QueryFilmsRequest, QueryFilmsResponse,
            },
            TypedService,
        },
        types_helper::map_schema,
    },
    CacheEndpoint, PipelineDetails,
};
use dozer_cache::cache::expression::{FilterExpression, QueryExpression};
use futures_util::FutureExt;
use std::{collections::HashMap, env, path::PathBuf, time::Duration};

use tokio::sync::{broadcast, oneshot};
use tokio_stream::StreamExt;
use tonic::{
    transport::{Endpoint, Server},
    Request,
};

use crate::test_utils;

use super::{
    super::utils::get_proto_descriptor,
    generated::films::{EventType, FilmEventRequest},
    test_utils::mock_event_notifier,
};
fn setup_typed_service() -> TypedService {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let schema_name = String::from("films");
    let (schema, _) = test_utils::get_schema();
    let endpoint = test_utils::get_endpoint();
    let pipeline_details = PipelineDetails {
        schema_name: schema_name.to_owned(),
        cache_endpoint: CacheEndpoint {
            cache: test_utils::initialize_cache(&schema_name, None),
            endpoint: endpoint.clone(),
        },
    };
    let path = out_dir
        .join("generated_films.bin")
        .to_string_lossy()
        .to_string();

    let desc = get_proto_descriptor(path).unwrap();

    let event_notifier = mock_event_notifier();
    let (tx, rx1) = broadcast::channel::<PipelineRequest>(16);
    ApiServer::setup_broad_cast_channel(tx, event_notifier).unwrap();
    let mut pipeline_map = HashMap::new();
    pipeline_map.insert("films".to_string(), pipeline_details);

    let mut schema_map = HashMap::new();
    schema_map.insert("films".to_string(), map_schema(endpoint.name, &schema));
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

    let request = FilmEventRequest {
        r#type: EventType::All as i32,
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
}
