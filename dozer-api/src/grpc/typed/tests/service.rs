use crate::{
    auth::{Access, Authorizer},
    generator::protoc::utils::get_proto_descriptor,
    grpc::{
        auth_middleware::AuthMiddlewareLayer,
        client_server::ApiServer,
        internal_grpc::PipelineResponse,
        typed::{
            tests::{
                fake_internal_pipeline_server::start_fake_internal_grpc_pipeline,
                generated::films::{
                    films_client::FilmsClient, CountFilmsResponse, FilmEvent, QueryFilmsRequest,
                    QueryFilmsResponse,
                },
            },
            TypedService,
        },
    },
    CacheEndpoint, PipelineDetails,
};
use dozer_cache::cache::expression::{FilterExpression, QueryExpression};
use dozer_types::{
    models::{api_config::default_api_config, api_security::ApiSecurity},
    types::Schema,
};
use futures_util::FutureExt;
use std::{collections::HashMap, env, path::PathBuf, str::FromStr, time::Duration};

use super::{generated::films::FilmEventRequest, types::EventType};
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
    metadata::MetadataValue,
    transport::{Endpoint, Server},
    Code, Request,
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

fn setup_typed_service(security: Option<ApiSecurity>) -> TypedService {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let path = out_dir.join("generated_films.bin");

    let (_, desc) = get_proto_descriptor(&path).unwrap();

    let (pipeline_map, schema_map, rx1) = setup_pipeline();

    TypedService::new(desc, pipeline_map, schema_map, Some(rx1), security)
}

async fn test_grpc_count_and_query_common(
    port: u32,
    request: QueryFilmsRequest,
    api_security: Option<ApiSecurity>,
    access_token: Option<String>,
) -> Result<(CountFilmsResponse, QueryFilmsResponse), tonic::Status> {
    let (sender_shutdown_internal, rx_internal) = oneshot::channel::<()>();
    let default_pipeline_internal = default_api_config().pipeline_internal.unwrap_or_default();
    let _jh1 = tokio::spawn(start_fake_internal_grpc_pipeline(
        default_pipeline_internal.host,
        default_pipeline_internal.port,
        rx_internal,
    ));

    let typed_service = setup_typed_service(api_security.to_owned());
    let (_tx, rx) = oneshot::channel::<()>();
    // middleware
    let layer = tower::ServiceBuilder::new()
        .layer(AuthMiddlewareLayer::new(api_security.to_owned()))
        .into_inner();
    let _jh = tokio::spawn(async move {
        Server::builder()
            .layer(layer)
            .add_service(typed_service)
            .serve_with_shutdown(
                format!("127.0.0.1:{:}", port).parse().unwrap(),
                rx.map(drop),
            )
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let channel = Endpoint::from_str(&format!("http://127.0.0.1:{:}", port))
        .unwrap()
        .connect()
        .await
        .unwrap();
    if api_security.is_some() {
        let my_token = access_token.unwrap_or_default();
        let mut client = FilmsClient::with_interceptor(channel, move |mut req: Request<()>| {
            let token: MetadataValue<_> = format!("Bearer {:}", my_token).parse().unwrap();
            req.metadata_mut().insert("authorization", token);
            Ok(req)
        });
        let res = client.count(Request::new(request.clone())).await?;
        let count_response = res.into_inner();
        let res = client.query(Request::new(request)).await?;
        let query_response = res.into_inner();
        _ = sender_shutdown_internal.send(());
        Ok((count_response, query_response))
    } else {
        let mut client = FilmsClient::new(channel);
        let res = client.count(Request::new(request.clone())).await?;
        let count_response = res.into_inner();
        let res = client.query(Request::new(request)).await?;
        let query_response = res.into_inner();
        _ = sender_shutdown_internal.send(());
        Ok((count_response, query_response))
    }
}

#[tokio::test]
async fn test_grpc_query() {
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

    let (count_response, query_response) =
        test_grpc_count_and_query_common(1402, request, None, None)
            .await
            .unwrap();
    assert_eq!(count_response.count, query_response.data.len() as u64);
    assert!(!query_response.data.len() > 0);
}

#[tokio::test]
async fn test_grpc_query_with_access_token() {
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
    let api_security = ApiSecurity::Jwt("DXkzrlnTy6".to_owned());
    let authorizer = Authorizer::from(api_security.to_owned());
    let generated_token = authorizer.generate_token(Access::All, None).unwrap();
    let (count_response, query_response) =
        test_grpc_count_and_query_common(1403, request, Some(api_security), Some(generated_token))
            .await
            .unwrap();
    assert_eq!(count_response.count, query_response.data.len() as u64);
    assert!(!query_response.data.is_empty());
}

#[tokio::test]
async fn test_grpc_query_with_wrong_access_token() {
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
    let api_security = ApiSecurity::Jwt("DXkzrlnTy6".to_owned());
    let generated_token = "wrongrandomtoken".to_owned();
    let request_response =
        test_grpc_count_and_query_common(1404, request, Some(api_security), Some(generated_token))
            .await;
    assert!(request_response.is_err());
    assert!(request_response.unwrap_err().code() == Code::PermissionDenied);
}

#[tokio::test]
async fn test_typed_streaming1() {
    let (sender_shutdown_internal, rx_internal) = oneshot::channel::<()>();
    let default_pipeline_internal = default_api_config().pipeline_internal.unwrap_or_default();
    let _jh1 = tokio::spawn(start_fake_internal_grpc_pipeline(
        default_pipeline_internal.host,
        default_pipeline_internal.port,
        rx_internal,
    ));
    let (_tx, rx) = oneshot::channel::<()>();
    let _jh = tokio::spawn(async move {
        let typed_service = setup_typed_service(None);
        Server::builder()
            .add_service(typed_service)
            .serve_with_shutdown("127.0.0.1:14321".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(1001)).await;
    let address = "http://127.0.0.1:14321".to_owned();
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
    _ = sender_shutdown_internal.send(());
}

#[tokio::test]
async fn test_typed_streaming2() {
    let (sender_shutdown_internal, rx_internal) = oneshot::channel::<()>();
    let default_pipeline_internal = default_api_config().pipeline_internal.unwrap_or_default();
    let _jh1 = tokio::spawn(start_fake_internal_grpc_pipeline(
        default_pipeline_internal.host,
        default_pipeline_internal.port,
        rx_internal,
    ));
    let (_tx, rx) = oneshot::channel::<()>();
    let _jh = tokio::spawn(async move {
        let typed_service = setup_typed_service(None);
        Server::builder()
            .add_service(typed_service)
            .serve_with_shutdown("127.0.0.1:14322".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(1001)).await;
    let address = "http://127.0.0.1:14322".to_owned();
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
    while let Some(item) = stream.next().await {
        let response: FilmEvent = item.unwrap();
        assert!(response.new.is_some());
    }
    _ = sender_shutdown_internal.send(());
}

#[tokio::test]
async fn test_typed_streaming3() {
    let (sender_shutdown_internal, rx_internal) = oneshot::channel::<()>();
    let default_pipeline_internal = default_api_config().pipeline_internal.unwrap_or_default();
    let _jh1 = tokio::spawn(start_fake_internal_grpc_pipeline(
        default_pipeline_internal.host,
        default_pipeline_internal.port,
        rx_internal,
    ));
    let (_tx, rx) = oneshot::channel::<()>();
    let _jh = tokio::spawn(async move {
        let typed_service = setup_typed_service(None);
        Server::builder()
            .add_service(typed_service)
            .serve_with_shutdown("127.0.0.1:14323".parse().unwrap(), rx.map(drop))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let address = "http://127.0.0.1:14323".to_owned();
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
    let error_timeout = timeout(Duration::from_secs(1), stream.next()).await;
    assert!(error_timeout.is_err() || error_timeout.unwrap().is_none());
    _ = sender_shutdown_internal.send(());
}
