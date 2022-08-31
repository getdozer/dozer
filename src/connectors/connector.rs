use async_trait::async_trait;

#[async_trait]
pub trait Connector {
    fn new(conn_str: String, tables: Option<Vec<String>>) -> Self;
    async fn connect(&mut self);
    async fn get_schema(&self);
    async fn start(&mut self);
    async fn stop(&self);
}
