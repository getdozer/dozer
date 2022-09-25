use crate::types::Schema;
use std::net::{IpAddr, Ipv6Addr};
pub use tarpc::{client, context, tokio_serde::formats::Json};

pub const SCHEMA_ADDR: IpAddr = IpAddr::V6(Ipv6Addr::LOCALHOST);
pub const SCHEMA_PORT: u16 = 8087;

#[tarpc::service]
pub trait SchemaRegistry {
    async fn ping() -> String;
    async fn insert(schema: Schema);
    async fn get(schema_id: u32) -> Schema;
}

pub async fn get_client() -> anyhow::Result<SchemaRegistryClient> {
    let server_addr = (SCHEMA_ADDR, SCHEMA_PORT);
    let transport = tarpc::serde_transport::tcp::connect(server_addr, Json::default);

    let client = SchemaRegistryClient::new(client::Config::default(), transport.await?).spawn();
    Ok(client)
}
