
use tonic::{transport::Server, Request, Response, Status};
use crate::connectors::connector::Connector;
use prost_types;
use dozer_shared::ingestion::{
  Connection,
  ConnectionResponse,
  ConnectionDetails,
  ingestion_server::Ingestion,
  ingestion_server::IngestionServer,
};
#[derive(Debug, Default)]
pub struct IngestionService {

}

#[tonic::async_trait]
impl Ingestion for IngestionService {
  async fn connect_db(&self, request: Request<Connection>) -> Result<Response<ConnectionResponse>, Status> {
    let connection_input:Connection = request.into_inner();
    // println!("Got a request: {:?}", connection_input);
    let connection_detail  = connection_input.detail.unwrap();
    println!("connection_detail port: {:?}", connection_detail.port);
    let port:u32 = connection_detail.port.to_string().trim().parse().unwrap();
    let client = crate::storage_client::initialize().await;
    let postgres_config = crate::connectors::postgres::connector::PostgresConfig {
      name: connection_detail.name,
      tables: None,
      conn_str: format!("host={} port={} user={} dbname={} password={}",connection_detail.host,port, connection_detail.user, connection_detail.database,connection_detail.password),
    };
    let mut connector = crate::connectors::postgres::connector::PostgresConnector::new(postgres_config, client);
    connector.initialize().await;

    let mut tables = Vec::new();
    tables.push(prost_types::Value {
      kind: Some(prost_types::value::Kind::StringValue(String::from("table1")))
    });
    let mut views = Vec::new();
    views.push(prost_types::Value {
      kind: Some(prost_types::value::Kind::StringValue(String::from("views1")))
    });

    Ok(Response::new(ConnectionResponse{
      response: Some(dozer_shared::ingestion::connection_response::Response::Success(ConnectionDetails{
        tables:  Some(prost_types::ListValue { values: tables }),
        views: Some(prost_types::ListValue { values: views })
      }))
    }))
  }
}


pub async fn get_server() -> Result<(), tonic::transport::Error> {
  let addr = "[::1]:8081".parse().unwrap();
  let my_ingestion = IngestionService::default();

  Server::builder()
      .add_service(IngestionServer::new(my_ingestion))
      .serve(addr)
      .await
}
