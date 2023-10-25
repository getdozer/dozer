use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Poll};

use dozer_ingestion_connector::{
    dozer_types::{self, bytes},
    futures::future::BoxFuture,
    futures::stream::BoxStream,
    futures::Stream,
    retry_on_network_failure,
    tokio::{self, sync::Mutex},
};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Config, CopyBothDuplex, Row, SimpleQueryMessage, Statement, ToStatement};

use crate::connection::helper::is_network_failure;
use crate::PostgresConnectorError;

use super::helper;

#[derive(Debug)]
pub struct Client {
    config: tokio_postgres::Config,
    inner: tokio_postgres::Client,
}

impl Client {
    pub fn new(config: Config, client: tokio_postgres::Client) -> Self {
        Self {
            config,
            inner: client,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn prepare(&mut self, query: &str) -> Result<Statement, tokio_postgres::Error> {
        retry_on_network_failure!(
            "prepare",
            self.inner.prepare(query).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn simple_query(
        &mut self,
        query: &str,
    ) -> Result<Vec<SimpleQueryMessage>, tokio_postgres::Error> {
        retry_on_network_failure!(
            "simple_query",
            self.inner.simple_query(query).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn query_one<T>(
        &mut self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, tokio_postgres::Error>
    where
        T: ?Sized + ToStatement,
    {
        retry_on_network_failure!(
            "query_one",
            self.inner.query_one(statement, params).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn query<T>(
        &mut self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, tokio_postgres::Error>
    where
        T: ?Sized + ToStatement,
    {
        retry_on_network_failure!(
            "query",
            self.inner.query(statement, params).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn query_raw(
        &mut self,
        query: String,
        params: Vec<String>,
    ) -> Result<BoxStream<'static, Result<Row, tokio_postgres::Error>>, tokio_postgres::Error> {
        let client = Self::connect(self.config.clone()).await?;
        let row_stream = RowStream::new(client, query, params).await?;
        Ok(Box::pin(row_stream))
    }

    pub async fn copy_both_simple<T>(
        &mut self,
        query: &str,
    ) -> Result<CopyBothDuplex<T>, tokio_postgres::Error>
    where
        T: bytes::Buf + 'static + Send,
    {
        retry_on_network_failure!(
            "copy_both_simple",
            self.inner.copy_both_simple(query).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn batch_execute(&mut self, query: &str) -> Result<(), tokio_postgres::Error> {
        retry_on_network_failure!(
            "batch_execute",
            self.inner.batch_execute(query).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn reconnect(&mut self) -> Result<(), tokio_postgres::Error> {
        let new_client = Self::connect(self.config.clone()).await?;
        self.inner = new_client.inner;
        Ok(())
    }

    pub async fn connect(config: Config) -> Result<Client, tokio_postgres::Error> {
        let client = match helper::connect(config).await {
            Ok(client) => client,
            Err(PostgresConnectorError::ConnectionFailure(err)) => return Err(err),
            Err(err) => panic!("unexpected error {err}"),
        };
        Ok(client)
    }

    async fn query_raw_internal(
        &mut self,
        statement: Statement,
        params: Vec<String>,
    ) -> Result<tokio_postgres::RowStream, tokio_postgres::Error> {
        retry_on_network_failure!(
            "query_raw",
            self.inner.query_raw(&statement, &params).await,
            is_network_failure,
            self.reconnect().await?
        )
    }
}

pub struct RowStream {
    client: Arc<Mutex<Client>>,
    query: String,
    query_params: Vec<String>,
    cursor_position: u64,
    inner: Pin<Box<tokio_postgres::RowStream>>,
    pending_resume: Option<
        BoxFuture<'static, Result<(Client, tokio_postgres::RowStream), tokio_postgres::Error>>,
    >,
}

impl RowStream {
    pub async fn new(
        mut client: Client,
        query: String,
        params: Vec<String>,
    ) -> Result<Self, tokio_postgres::Error> {
        let statement = client.prepare(&query).await?;
        let inner = client.query_raw_internal(statement, params.clone()).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            query,
            query_params: params,
            cursor_position: 0,
            inner: Box::pin(inner),
            pending_resume: None,
        })
    }

    fn resume(
        &mut self,
    ) -> BoxFuture<'static, Result<(Client, tokio_postgres::RowStream), tokio_postgres::Error>>
    {
        async fn resume_async(
            client: Arc<Mutex<Client>>,
            query: String,
            params: Vec<String>,
            offset: u64,
        ) -> Result<(Client, tokio_postgres::RowStream), tokio_postgres::Error> {
            let config = client.lock().await.config().clone();
            // reconnect
            let mut client = Client::connect(config).await?;
            // send query with offset
            let statement = client.prepare(&add_query_offset(&query, offset)).await?;
            let row_stream = client.query_raw_internal(statement, params).await?;
            Ok((client, row_stream))
        }

        Box::pin(resume_async(
            self.client.clone(),
            self.query.clone(),
            self.query_params.clone(),
            self.cursor_position,
        ))
    }
}

impl Stream for RowStream {
    type Item = Result<Row, tokio_postgres::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(resume) = this.pending_resume.as_mut() {
                match ready!(resume.as_mut().poll(cx)) {
                    Ok((client, inner)) => {
                        this.pending_resume = None;
                        this.client = Arc::new(Mutex::new(client));
                        this.inner = Box::pin(inner);
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                }
            }

            match ready!(this.inner.as_mut().poll_next(cx)) {
                Some(Ok(row)) => {
                    this.cursor_position += 1;
                    return Poll::Ready(Some(Ok(row)));
                }
                Some(Err(err)) => {
                    if is_network_failure(&err) {
                        this.pending_resume = Some(this.resume());
                        continue;
                    } else {
                        return Poll::Ready(Some(Err(err)));
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

fn add_query_offset(query: &str, offset: u64) -> String {
    assert!(query
        .trim_start()
        .get(0..7)
        .map(|s| s.to_uppercase() == "SELECT ")
        .unwrap_or(false));

    if offset == 0 {
        query.into()
    } else {
        format!("{query} OFFSET {offset}")
    }
}
