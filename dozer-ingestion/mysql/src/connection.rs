use dozer_ingestion_connector::{
    dozer_types, retry_on_network_failure,
    tokio::{self, sync::mpsc::Receiver},
};
use mysql_async::{prelude::*, BinlogStream, Params, Pool, Row};

#[derive(Debug)]
pub struct Conn {
    pool: mysql_async::Pool,
    inner: mysql_async::Conn,
}

impl Conn {
    pub async fn new(pool: mysql_async::Pool) -> Result<Conn, mysql_async::Error> {
        let conn = new_mysql_connection(&pool).await?;
        Ok(Conn { pool, inner: conn })
    }

    pub async fn exec_first<'a: 'b, 'b, T, P>(
        &'a mut self,
        query: &str,
        params: P,
    ) -> Result<Option<T>, mysql_async::Error>
    where
        P: Into<Params> + Send + Copy + 'b,
        T: FromRow + Send + 'static,
    {
        retry_on_network_failure!(
            "query",
            self.inner.exec_first(query, params).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub fn exec_iter(&mut self, query: String, params: Vec<mysql_async::Value>) -> QueryResult {
        exec_iter_impl(self.pool.clone(), query, params)
    }

    #[allow(unused)]
    pub async fn exec_drop<'a: 'b, 'b, P>(
        &'a mut self,
        query: &str,
        params: P,
    ) -> Result<(), mysql_async::Error>
    where
        P: Into<Params> + Send + Copy + 'b,
    {
        retry_on_network_failure!(
            "query",
            self.inner.exec_drop(query, params).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn query_drop(&mut self, query: &str) -> Result<(), mysql_async::Error> {
        retry_on_network_failure!(
            "query",
            self.inner.query_drop(query).await,
            is_network_failure,
            self.reconnect().await?
        )
    }

    pub async fn get_binlog_stream(
        self,
        server_id: u32,
        filename: &[u8],
        pos: u64,
    ) -> Result<BinlogStream, mysql_async::Error> {
        let mut inner = self.inner;
        retry_on_network_failure!(
            "get_binlog_stream",
            {
                let request = mysql_async::BinlogStreamRequest::new(server_id)
                    .with_filename(filename)
                    .with_pos(pos);
                inner.get_binlog_stream(request).await
            },
            is_network_failure,
            inner = new_mysql_connection(&self.pool).await?
        )
    }

    async fn reconnect(&mut self) -> Result<(), mysql_async::Error> {
        self.inner = new_mysql_connection(&self.pool).await?;
        Ok(())
    }
}

async fn new_mysql_connection(pool: &Pool) -> Result<mysql_async::Conn, mysql_async::Error> {
    retry_on_network_failure!("connect", pool.get_conn().await, is_network_failure)
}

pub fn is_network_failure(err: &mysql_async::Error) -> bool {
    use mysql_async::DriverError::*;
    use mysql_async::Error::*;
    matches!(err, Driver(ConnectionClosed) | Io(_))
}

fn add_query_offset(query: &str, offset: u64) -> String {
    assert!([(7, "SELECT ".to_string()), (5, "SHOW ".to_string())]
        .iter()
        .any(|(len, prefix)| {
            query
                .trim_start()
                .get(0..*len)
                .map(|s| s.to_uppercase() == *prefix)
                .unwrap_or(false)
        }));

    if offset == 0 {
        query.into()
    } else {
        format!("{query} LIMIT {offset},18446744073709551615")
    }
}

fn exec_iter_impl(pool: Pool, query: String, params: Vec<mysql_async::Value>) -> QueryResult {
    // this is basically a generator/coroutine using a channel to communicate the results
    let (sender, receiver) = tokio::sync::mpsc::channel(10);

    tokio::spawn(async move {
        let mut cursor_position: u64 = 0;
        'main: loop {
            let mut conn = match new_mysql_connection(&pool).await {
                Ok(conn) => conn,
                Err(err) => {
                    let _ = sender.send(Err(err)).await;
                    break;
                }
            };
            let mut rows = match retry_on_network_failure!(
                "query",
                conn.exec_iter(add_query_offset(&query, cursor_position), &params)
                    .await,
                is_network_failure,
                continue 'main
            ) {
                Ok(rows) => rows,
                Err(err) => {
                    let _ = sender.send(Err(err)).await;
                    break;
                }
            };
            loop {
                let result = retry_on_network_failure!(
                    "query",
                    rows.next().await,
                    is_network_failure,
                    continue 'main
                );
                let stop = result.is_err() || result.as_ref().unwrap().is_none();
                if sender.send(result).await.is_err() {
                    break;
                }
                if stop {
                    break;
                }
                cursor_position += 1;
            }
            break;
        }
    });

    QueryResult::new(receiver)
}

pub struct QueryResult {
    receiver: Receiver<Result<Option<Row>, mysql_async::Error>>,
}

impl QueryResult {
    pub fn new(receiver: Receiver<Result<Option<Row>, mysql_async::Error>>) -> Self {
        Self { receiver }
    }

    pub async fn next(&mut self) -> Option<Result<Row, mysql_async::Error>> {
        self.receiver.recv().await?.transpose()
    }

    pub async fn map<F, U>(&mut self, mut fun: F) -> Result<Vec<U>, mysql_async::Error>
    where
        F: FnMut(Row) -> U,
    {
        let mut acc = Vec::new();
        while let Some(result) = self.next().await {
            let row = result?;
            acc.push(fun(mysql_async::from_row(row)));
        }
        Ok(acc)
    }

    pub async fn reduce<T, F, U>(
        &mut self,
        mut init: U,
        mut fun: F,
    ) -> Result<U, mysql_async::Error>
    where
        F: FnMut(U, T) -> U,
        T: FromRow + Send + 'static,
    {
        while let Some(result) = self.next().await {
            let row = result?;
            init = fun(init, mysql_async::from_row(row));
        }
        Ok(init)
    }
}
