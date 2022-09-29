use crate::storage::{RocksConfig, RocksStorage, Storage};
use anyhow::Result;
use dozer_types::types::{Schema, SchemaIdentifier};
use futures::{future, prelude::*};
use std::net::{IpAddr, Ipv6Addr};
use std::{net::SocketAddr, sync::Arc};
use tarpc::server::{self, incoming::Incoming, Channel};
use tarpc::transport::channel::UnboundedChannel;
pub use tarpc::{client, context, tokio_serde::formats::Json};
use tarpc::{ClientMessage, Response};

pub const SCHEMA_ADDR: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);
pub const SCHEMA_PORT: u16 = 9005;

#[tarpc::service]
pub trait SchemaRegistry {
    async fn ping() -> String;
    async fn insert(schema: Schema);
    async fn get(schema_id: SchemaIdentifier) -> Schema;
}

pub async fn _get_client() -> anyhow::Result<SchemaRegistryClient> {
    let server_addr = (SCHEMA_ADDR, SCHEMA_PORT);
    let transport = tarpc::serde_transport::tcp::connect(server_addr, Json::default);

    let client = SchemaRegistryClient::new(client::Config::default(), transport.await?).spawn();
    Ok(client)
}

#[derive(Clone)]
pub struct SchemaRegistryServer {
    _addr: Option<SocketAddr>,
    storage_client: Arc<RocksStorage>,
}

#[tarpc::server]
impl SchemaRegistry for SchemaRegistryServer {
    async fn ping(self, _: tarpc::context::Context) -> String {
        "pong".to_string()
    }
    async fn insert(self, _: tarpc::context::Context, schema: dozer_types::types::Schema) {
        let client = self.storage_client.clone();
        client.insert_schema(&schema);
    }

    async fn get(self, _: tarpc::context::Context, schema_id: SchemaIdentifier) -> Schema {
        let client = self.storage_client.clone();
        client.get_schema(schema_id)
    }
}
pub fn _serve_channel() -> anyhow::Result<
    UnboundedChannel<Response<SchemaRegistryResponse>, ClientMessage<SchemaRegistryRequest>>,
> {
    let storage_client = Arc::new(Storage::new(RocksConfig::_default()));

    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();

    let channel = server::BaseChannel::with_defaults(server_transport);
    let server = SchemaRegistryServer {
        _addr: None,
        storage_client,
    };

    tokio::spawn(channel.execute(server.serve()));

    Ok(client_transport)
}

pub async fn _serve(config: Option<RocksConfig>) -> anyhow::Result<()> {
    let server_addr = (SCHEMA_ADDR, SCHEMA_PORT);

    let storage_config = match config {
        Some(config) => config,
        None => RocksConfig::_default(),
    };

    let storage_client = Arc::new(Storage::new(storage_config));

    println!("Schema Registry Listening on : {}", SCHEMA_PORT);
    let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        // Ignore accept errors.
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        // serve is generated by the service attribute. It takes as input any type implementing
        // the generated World trait.
        .map(|channel| {
            let server = SchemaRegistryServer {
                _addr: Some(channel.transport().peer_addr().unwrap()),
                storage_client: Arc::clone(&storage_client),
            };
            channel.execute(server.serve())
        })
        // Max 10 channels.
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::{client, context, SchemaRegistryClient};
    use dozer_types::types::{FieldDefinition, Schema, SchemaIdentifier};
    use std::{thread, time};
    use tokio::runtime::Runtime;

    use super::{_get_client, _serve, _serve_channel};

    async fn _run_test(client: SchemaRegistryClient) {
        let schema = Schema {
            identifier: Some(SchemaIdentifier { id: 1, version: 1 }),
            fields: vec![FieldDefinition {
                name: "foo".to_string(),
                typ: dozer_types::types::FieldType::String,
                nullable: true,
            }],
            values: vec![0],
            primary_index: vec![0],
            secondary_indexes: vec![],
        };
        // insert schema

        let str = client.ping(context::current()).await.unwrap();
        client
            .insert(context::current(), schema.clone())
            .await
            .unwrap();

        let result = client
            .get(context::current(), schema.identifier.clone().unwrap())
            .await
            .unwrap();
        assert_eq!(result, schema.clone());
    }

    #[tokio::test]
    async fn ping_pong() {
        let client_transport = _serve_channel().unwrap();

        let client = SchemaRegistryClient::new(client::Config::default(), client_transport).spawn();

        let pong = client.ping(context::current()).await.unwrap();
        assert_eq!(pong, "pong".to_string());
    }
    #[tokio::test]
    async fn insert_and_get_schema_channel() {
        let client_transport = _serve_channel().unwrap();

        let client = SchemaRegistryClient::new(client::Config::default(), client_transport).spawn();
        _run_test(client).await;
    }

    #[tokio::test]
    async fn insert_and_get_schema_tcp() {
        thread::spawn(|| {
            Runtime::new().unwrap().block_on(async {
                tokio::spawn(_serve(None)).await.unwrap().unwrap();
            });
        });

        let ten_millis = time::Duration::from_millis(100);
        thread::sleep(ten_millis);

        let client = _get_client().await.unwrap();

        _run_test(client).await;
    }
}
