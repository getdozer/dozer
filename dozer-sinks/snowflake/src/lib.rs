use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroUsize,
    sync::Arc,
    thread,
    time::{Duration, Instant},
    usize,
};

use dozer_api::shutdown::ShutdownReceiver;
use dozer_log::{
    reader::LogClient,
    replication::LogOperation,
    schemas::EndpointSchema,
    tokio::{self, runtime::Runtime},
};
use dozer_types::{
    errors::internal::BoxedError,
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient, LogRequest,
    },
    json_types::json_to_string,
    log::{debug, error, info},
    models::sink_config::{
        self,
        snowflake::{self, default_batch_interval, default_batch_size, default_suspend_warehouse},
    },
    rust_decimal::Decimal,
    types::{DozerDuration, Field, FieldDefinition, FieldType, Record, Schema},
};
use futures_util::future;
use itertools::Itertools;
use odbc_api::{
    buffers::{AnyBuffer, BufferDesc},
    handles::{CData, HasDataType, Statement},
    parameter::{CElement, VarBinaryBox, VarCharBox},
    Bit, ConnectionOptions, IntoParameter, Nullable, ParameterCollection,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Debug)]
pub struct SnowflakeSink {
    config: sink_config::Snowflake,
    app_server_url: String,
}

impl SnowflakeSink {
    pub fn new(config: sink_config::Snowflake, app_server_url: String) -> Self {
        Self {
            config,
            app_server_url,
        }
    }

    pub async fn run(&mut self, shutdown: ShutdownReceiver) {
        info!(
            "Starting Snowflake Sink for endpoint: {}",
            self.config.endpoint
        );

        macro_rules! try_await {
            ($future:expr) => {
                tokio::select! {
                    result = $future => result,
                    _ = shutdown.create_shutdown_future() => return,
                }
            };
        }

        let endpoint = &self.config.endpoint;
        let LogConnection {
            endpoint_schema, ..
        } = retry_internal_pipeline_connect(self.app_server_url.clone(), endpoint.clone()).await;

        let (query_sender, query_receiver) = channel(1);
        let (result_sender, mut result_receiver) = channel(1);
        thread::spawn({
            let config = self.config.clone();
            || snowflake_client(query_receiver, result_sender, config)
        });

        macro_rules! execute_queries {
            ($queries:expr) => {{
                let queries = $queries;
                query_sender
                    .send(queries)
                    .await
                    .expect("query executor crashed");

                let result = result_receiver
                    .recv()
                    .await
                    .expect("query executor crashed");

                result.expect("Snowflake error: {result:?}");
            }};
        }

        let schema = endpoint_schema.schema;
        execute_queries!(self.ddl_queries(&schema));

        let options = self.config.options.clone().unwrap_or_default();
        let batch_size = options.batch_size.unwrap_or_else(default_batch_size);
        let batch_interval = options
            .batch_interval_seconds
            .unwrap_or_else(default_batch_interval);
        let suspend_warehouse = options
            .suspend_warehouse_after_each_batch
            .unwrap_or_else(default_suspend_warehouse);

        let (log_sender, mut log_receiver) = channel(500);
        thread::spawn({
            let app_server_url = self.app_server_url.clone();
            let endpoint = endpoint.clone();
            let shutdown = shutdown.clone();
            move || {
                log_reader(
                    app_server_url,
                    endpoint,
                    batch_size as u64,
                    log_sender,
                    shutdown,
                )
            }
        });

        let mut processed_ops = 0;
        let mut batch_start_time = Instant::now();
        'main: loop {
            let batch_time = Instant::now() - batch_start_time;
            debug!(
                "Batch cycle.\n  total operations: {};\n  total time: {} hours, {} minutes, {} seconds {} milliseconds\n  rate: {} op/sec",
                processed_ops,
                batch_time.as_secs() / 3600,
                (batch_time.as_secs() % 3600) / 60,
                batch_time.as_secs() % 60,
                batch_time.subsec_millis(),
                processed_ops as f64 / batch_time.as_secs_f64()
            );

            let mut batch = Vec::new();

            loop {
                let capacity = 10;
                let mut results = Vec::with_capacity(capacity);
                let num_results = try_await!(log_receiver.recv_many(&mut results, capacity));
                if num_results == 0 {
                    panic!("log reader crashed");
                }
                let mut continue_reading = true;
                for result in results {
                    match result {
                        LogReaderResult::Ops(ops) => {
                            batch.extend(ops);
                            continue_reading = true;
                        }
                        LogReaderResult::Flush => continue_reading = false,
                        LogReaderResult::Reset => {
                            info!("Pipeline was restarted from scratch. The Snowflake sink for endpoint {endpoint} will restart as well.");
                            execute_queries!(self.ddl_queries(&schema));
                            batch_start_time = Instant::now();
                            continue 'main;
                        }
                    }
                }
                if !continue_reading || batch.len() >= batch_size {
                    break;
                }
            }

            if batch.is_empty() {
                let batch_time = Instant::now() - batch_start_time;
                debug!(
                    "Batch finished!\n  total operations: {};\n  total time: {} hours, {} minutes, {} seconds {} milliseconds\n  rate: {} op/sec",
                    processed_ops,
                    batch_time.as_secs() / 3600,
                    (batch_time.as_secs() % 3600) / 60,
                    batch_time.as_secs() % 60,
                    batch_time.subsec_millis(),
                    processed_ops as f64 / batch_time.as_secs_f64()
                );
                debug!(
                    "No more pending log operations. Sleeping for {} seconds.",
                    batch_interval.as_secs()
                );
                if suspend_warehouse && processed_ops > 0 {
                    execute_queries!(vec![format!(
                        "ALTER WAREHOUSE {} SUSPEND",
                        ident(&self.config.connection.warehouse)
                    )
                    .into()]);
                }
                try_await!(tokio::time::sleep(batch_interval));
                processed_ops = 0;
                continue;
            }

            processed_ops += batch.len();

            let mut ops = batch
                .into_iter()
                .filter(|op| matches!(op, LogOperation::Op { .. }))
                .collect::<Vec<_>>();

            for chunk in ops.chunks_mut(batch_size) {
                debug!(
                    "Applying {} log operations for endpoint {endpoint} to Snowflake sink",
                    chunk.len()
                );

                let dml_queries = self.operations_to_dml(&schema, chunk);
                execute_queries!(dml_queries);

                debug!(
                    "Applied {} log operations to Snowflake sink for endpoint {endpoint}",
                    chunk.len()
                );
            }
        }
    }

    fn ddl_queries(&self, endpoint_schema: &Schema) -> Vec<QueryWithParams> {
        let snowflake::Destination {
            database,
            schema,
            table,
        } = &self.config.destination;

        let object_name = format!("{}.{}.{}", ident(database), ident(schema), ident(table));

        vec![
            format!("DROP TABLE IF EXISTS {}", object_name),
            format!("CREATE TABLE IF NOT EXISTS {}({})", object_name, {
                let mut columns = String::new();
                columns.push_str(
                    endpoint_schema
                        .fields
                        .iter()
                        .map(|f| {
                            format!(
                                "{} {}{}",
                                f.name,
                                field_type_to_snowflake_sql_type(f.typ),
                                if !f.nullable { " NOT NULL" } else { "" }
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                        .as_str(),
                );
                if !endpoint_schema.primary_index.is_empty() {
                    columns.push_str(", ");
                    columns.push_str(&format!(
                        "PRIMARY KEY ({})",
                        endpoint_schema
                            .primary_index
                            .iter()
                            .copied()
                            .map(|i| endpoint_schema.fields[i].name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                columns
            }),
        ]
        .into_iter()
        .map(|q| QueryWithParams::new(q, QueryParams::None))
        .collect()
    }

    fn operations_to_dml(
        &self,
        endpoint_schema: &Schema,
        operations: &mut [LogOperation],
    ) -> Vec<QueryWithParams> {
        struct QueryBuilder<'a> {
            schema: &'a Schema,
            pub queries: Vec<QueryWithParams>,
        }

        impl<'a> QueryBuilder<'a> {
            fn new(schema: &'a Schema) -> Self {
                Self {
                    schema,
                    queries: Vec::new(),
                }
            }

            fn where_clause(&mut self, record: &Record, params: &mut Vec<OdbcParam>) -> String {
                let fields = record.values.iter().enumerate();
                if !self.schema.primary_index.is_empty() {
                    fields
                        .filter(|(i, _)| self.schema.primary_index.contains(i))
                        .map(|(i, f)| self.format_kv(i, f, params))
                        .collect::<Vec<_>>()
                } else {
                    fields
                        .map(|(i, f)| self.format_kv(i, f, params))
                        .collect::<Vec<_>>()
                }
                .join(" AND ")
            }

            fn format_kv(
                &mut self,
                field_index: usize,
                field_value: &Field,
                params: &mut Vec<OdbcParam>,
            ) -> String {
                params.push(field_to_query_param(field_value));
                format!("{} = ?", self.schema.fields[field_index].name)
            }

            pub fn delete(&mut self, table: &str, records: &[&Record]) {
                debug_assert_eq!(records.len(), 1);
                let mut params = Vec::new();
                let query = format!(
                    "DELETE FROM {table} WHERE {}",
                    if records.len() == 1 {
                        self.where_clause(records[0], &mut params)
                    } else {
                        records
                            .iter()
                            .map(|record| format!("({})", self.where_clause(record, &mut params)))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    }
                );
                self.queries.push(QueryWithParams::new(
                    query,
                    QueryParams::SingleRowParams(params),
                ));
            }

            pub fn insert(&mut self, table: &str, records: &mut [&mut Record]) {
                debug_assert_ne!(records.len(), 0);
                let query = format!(
                    "INSERT INTO {table}({}) VALUES({})",
                    self.schema
                        .fields
                        .iter()
                        .map(|f| f.name.as_str())
                        .collect_vec()
                        .join(", "),
                    std::iter::repeat("?")
                        .take(self.schema.fields.len())
                        .collect_vec()
                        .join(", ")
                );
                let params = if records.len() == 1 {
                    QueryParams::SingleRowParams(
                        records[0].values.iter().map(field_to_query_param).collect(),
                    )
                } else {
                    record_batch_to_param_batch(self.schema, records)
                };
                self.queries.push(QueryWithParams::new(query, params));
            }

            pub fn update(&mut self, table: &str, old: &Record, new: &Record) {
                let mut params = Vec::new();
                let query = format!(
                    "UPDATE {table} SET {} WHERE {}",
                    new.values
                        .iter()
                        .enumerate()
                        .map(|(i, f)| self.format_kv(i, f, &mut params))
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.where_clause(old, &mut params)
                );
                self.queries.push(QueryWithParams::new(
                    query,
                    QueryParams::SingleRowParams(params),
                ));
            }
        }

        let mut query_builder = QueryBuilder::new(endpoint_schema);

        let table = {
            let snowflake::Destination {
                database,
                schema,
                table,
            } = &self.config.destination;
            format!("{}.{}.{}", ident(database), ident(schema), ident(table))
        };

        let mut delete = Vec::new();
        let mut insert = Vec::new();

        #[derive(Debug, Clone, Copy)]
        enum OpKind {
            Delete,
            Insert,
            Update,
            None,
        }
        impl OpKind {
            fn is_delete(&self) -> bool {
                matches!(self, Self::Delete)
            }
            fn is_insert(&self) -> bool {
                matches!(self, Self::Insert)
            }
            fn is_update(&self) -> bool {
                matches!(self, Self::Update)
            }
        }

        let mut previous_op_kind = OpKind::None;

        macro_rules! flush {
            () => {{
                match previous_op_kind {
                    OpKind::Delete => {
                        query_builder.delete(&table, &delete);
                        delete.clear();
                    }
                    OpKind::Insert => {
                        query_builder.insert(&table, &mut insert);
                        insert.clear();
                    }
                    OpKind::Update | OpKind::None => (),
                }
                previous_op_kind = OpKind::None;
                let _ = previous_op_kind;
            }};
        }

        for op in operations {
            match op {
                LogOperation::Op { op } => {
                    use dozer_types::types::Operation;
                    match op {
                        Operation::Delete { old } => {
                            if !previous_op_kind.is_delete() {
                                flush!()
                            }
                            previous_op_kind = OpKind::Delete;
                            delete.push(old);
                        }
                        Operation::Insert { new } => {
                            if !previous_op_kind.is_insert() {
                                flush!()
                            }
                            previous_op_kind = OpKind::Insert;
                            insert.push(new);
                        }
                        Operation::Update { old, new } => {
                            if !previous_op_kind.is_update() {
                                flush!()
                            }
                            previous_op_kind = OpKind::Update;
                            query_builder.update(&table, old, new);
                        }
                        Operation::BatchInsert { new } => {
                            if !previous_op_kind.is_insert() {
                                flush!()
                            }
                            previous_op_kind = OpKind::Insert;
                            insert.extend(new);
                        }
                    }
                }
                LogOperation::Commit { .. }
                | LogOperation::SnapshottingStarted { .. }
                | LogOperation::SnapshottingDone { .. } => {
                    unreachable!("should've been filtered out earlier")
                }
            }
        }
        flush!();

        query_builder.queries
    }
}

struct LogConnection {
    log_client: LogClient,
    endpoint_schema: EndpointSchema,
    server_id: String,
}

async fn try_internal_pipeline_connect(
    app_server_url: String,
    endpoint: String,
) -> Result<LogConnection, BoxedError> {
    let mut client = InternalPipelineServiceClient::connect(app_server_url.clone()).await?;
    let server_id = client.get_id(()).await?.into_inner().id;
    let (log_client, endpoint_schema) = LogClient::new(&mut client, endpoint).await?;
    Ok(LogConnection {
        log_client,
        endpoint_schema,
        server_id,
    })
}

async fn retry_internal_pipeline_connect(
    app_server_url: String,
    endpoint: String,
) -> LogConnection {
    loop {
        match try_internal_pipeline_connect(app_server_url.clone(), endpoint.clone()).await {
            Ok(result) => break result,
            Err(err) => {
                const RETRY_INTERVAL: Duration = Duration::from_secs(5);
                error!(
                    "while connecting to internal pipeline server: {}. Retrying in {:?}",
                    err, RETRY_INTERVAL
                );
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }
    }
}

enum LogReaderResult {
    Ops(Vec<LogOperation>),
    Flush,
    Reset,
}

fn log_reader(
    app_server_url: String,
    endpoint: String,
    max_batch_size: u64,
    sender: Sender<LogReaderResult>,
    shutdown: ShutdownReceiver,
) {
    let runtime = Arc::new(Runtime::new().unwrap());

    macro_rules! try_await {
        ($future:expr) => {{
            let fut = $future;
            let shutdown = shutdown.create_shutdown_future();
            tokio::pin!(fut);
            tokio::pin!(shutdown);
            match runtime.block_on(future::select(fut, shutdown)) {
                future::Either::Left((result, _)) => result,
                future::Either::Right(((), _)) => return,
            }
        }};
    }

    macro_rules! try_send {
        ($value:expr) => {
            if let Err(_) = sender.blocking_send($value) {
                return;
            }
        };
    }

    let LogConnection {
        mut log_client,
        mut server_id,
        ..
    } = try_await!(retry_internal_pipeline_connect(
        app_server_url.clone(),
        endpoint.clone()
    ));

    let log_reader_batch_size = std::cmp::min(max_batch_size, 4000);
    let timeout_in_millis = 300;
    let mut current_end = 0;

    use LogReaderResult::*;

    loop {
        let start = current_end;
        let end = start + log_reader_batch_size;
        let request = LogRequest {
            endpoint: endpoint.clone(),
            start,
            end,
            timeout_in_millis,
        };
        debug!("sending log read request {request:?}");

        let batch = match try_await!(log_client.get_log(request)) {
            Ok(ops) => ops,
            Err(err) => {
                error!("log read error: {err}");
                let connection = try_await!(retry_internal_pipeline_connect(
                    app_server_url.clone(),
                    endpoint.clone()
                ));
                log_client = connection.log_client;
                if server_id != connection.server_id {
                    server_id = connection.server_id;
                    info!("Pipeline was restarted from scratch. The Snowflake sink for endpoint {endpoint} will restart as well.");
                    // reset offset
                    current_end = 0;
                    try_send!(Reset);
                }
                continue;
            }
        };
        let batch_len = batch.len() as u64;
        current_end += batch_len;

        debug!("got {} ops", batch_len);

        try_send!(Ops(batch));

        if batch_len < log_reader_batch_size {
            try_send!(Flush);
        }
    }
}

struct QueryWithParams {
    pub query: String,
    pub params: QueryParams,
}

enum QueryParams {
    None,
    SingleRowParams(Vec<OdbcParam>),
    ColumnarParams {
        schema: Vec<BufferDesc>,
        columns: Vec<AnyBuffer>,
        num_rows: usize,
    },
}

impl QueryWithParams {
    pub fn new(query: String, params: QueryParams) -> Self {
        Self { query, params }
    }
}

impl From<String> for QueryWithParams {
    fn from(query: String) -> Self {
        Self {
            query,
            params: QueryParams::None,
        }
    }
}

fn snowflake_client(
    mut query_receiver: Receiver<Vec<QueryWithParams>>,
    result_sender: Sender<Result<(), BoxedError>>,
    config: sink_config::Snowflake,
) {
    let env = odbc_api::Environment::new().unwrap();
    let conn = env
        .connect_with_connection_string(
            &connection_string_from_config(config.clone()),
            ConnectionOptions::default(),
        )
        .expect("failed to connect to snowflake");

    conn.execute(
        &format!("use database {}", ident(&config.destination.database)),
        (),
    )
    .expect("failed to setup snowflake ODBC session");

    macro_rules! try_exec {
        ($query:expr, $params:expr) => {{
            let query = $query;
            let params = $params;
            debug!("Executing query {:?} with params {:?}", query, params);
            if let Err(err) = conn.execute(query, params) {
                error!("Error executing query {:?}: {err:?}", query);
                let Ok(()) = result_sender.blocking_send(Err(err.into())) else {
                    break;
                };
                continue;
            }
        }};
    }

    loop {
        let Some(sql_queries) = query_receiver.blocking_recv() else {
            break;
        };
        let multiple_queries = sql_queries.len() > 1;
        if multiple_queries {
            try_exec!("BEGIN", ());
        }
        for QueryWithParams { query, params } in sql_queries {
            match params {
                QueryParams::None => try_exec!(&query, ()),
                QueryParams::SingleRowParams(params) => try_exec!(&query, &mut ParamVec(params)),
                QueryParams::ColumnarParams {
                    schema,
                    columns,
                    num_rows,
                } => {
                    debug_assert_ne!(columns.len(), 0);
                    debug_assert_eq!(columns.len(), schema.len());
                    try_exec!(&query, &mut ParamBinder::new(columns, num_rows));
                }
            }
        }
        if multiple_queries {
            try_exec!("COMMIT", ());
        }
        let Ok(()) = result_sender.blocking_send(Ok(())) else {
            break;
        };
    }
}

fn connection_string_from_config(config: sink_config::Snowflake) -> String {
    let conn = config.connection;
    let dest = config.destination;
    let mut conn_hashmap: HashMap<String, String> = HashMap::new();
    let driver = match &conn.driver {
        None => "Snowflake".to_string(),
        Some(driver) => driver.to_string(),
    };

    conn_hashmap.insert("Driver".to_string(), driver);
    conn_hashmap.insert("Server".to_string(), conn.server);
    conn_hashmap.insert(
        "Port".to_string(),
        conn.port.unwrap_or_else(|| "443".to_string()),
    );
    conn_hashmap.insert("Uid".to_string(), conn.user);
    conn_hashmap.insert("Pwd".to_string(), conn.password);
    conn_hashmap.insert("Warehouse".to_string(), conn.warehouse);
    conn_hashmap.insert("Schema".to_string(), dest.schema);
    conn_hashmap.insert("Database".to_string(), dest.database);
    if let Some(role) = conn.role {
        conn_hashmap.insert("Role".to_string(), role);
    }

    let mut parts = vec![];
    conn_hashmap.keys().for_each(|k| {
        parts.push(format!("{}={}", k, conn_hashmap.get(k).unwrap()));
    });

    let connection_string = parts.join(";");

    debug!("Snowflake connection string: {:?}", connection_string);
    connection_string
}

fn field_type_to_snowflake_sql_type(field_type: FieldType) -> String {
    match field_type {
        FieldType::UInt => "INTEGER",
        FieldType::U128 => "INTEGER",
        FieldType::Int => "INTEGER",
        FieldType::I128 => "INTEGER",
        FieldType::Float => "FLOAT",
        FieldType::Boolean => "BOOLEAN",
        FieldType::String => "TEXT",
        FieldType::Text => "TEXT",
        FieldType::Binary => "BINARY",
        FieldType::Decimal => "DECIMAL(38, 10)",
        FieldType::Timestamp => "TIMESTAMP",
        FieldType::Date => "DATE",
        FieldType::Json => "TEXT",
        FieldType::Point => "BINARY",
        FieldType::Duration => "TIMESTAMP",
    }
    .to_string()
}

fn field_to_query_param(field: &Field) -> OdbcParam {
    match field {
        Field::UInt(u) => OdbcParam::U64(*u),
        Field::U128(u) => OdbcParam::String(u.to_string().into_parameter()),
        Field::Int(i) => OdbcParam::I64(*i),
        Field::I128(i) => OdbcParam::String(i.to_string().into_parameter()),
        Field::Float(f) => OdbcParam::F64(f.0),
        Field::Boolean(b) => OdbcParam::Bool(Bit::from_bool(*b)),
        Field::String(s) => OdbcParam::String(s.clone().into_parameter()),
        Field::Text(s) => OdbcParam::String(s.clone().into_parameter()),
        Field::Binary(b) => OdbcParam::Binary(b.clone().into_parameter()),
        Field::Decimal(d) => OdbcParam::Decimal(d.to_string().into_parameter()),
        Field::Timestamp(t) => OdbcParam::Timestamp(t.to_string().into_parameter()),
        Field::Date(d) => OdbcParam::Date(d.to_string().into_parameter()),
        Field::Json(j) => OdbcParam::String(json_to_string(j).into_parameter()),
        Field::Point(p) => OdbcParam::Binary(p.to_bytes().to_vec().into_parameter()),
        Field::Duration(d) => OdbcParam::Timestamp(duration_to_timestamp(d).into_parameter()),
        Field::Null => OdbcParam::Null(Nullable::null()),
    }
}

fn duration_to_timestamp(duration: &DozerDuration) -> String {
    ("0001-01-01T00:00:00Z"
        .parse::<chrono::DateTime<chrono::Utc>>()
        .unwrap()
        + duration.0)
        .to_string()
}

fn record_batch_to_param_batch(dozer_schema: &Schema, records: &mut [&mut Record]) -> QueryParams {
    let num_columns = dozer_schema.fields.len();
    let num_rows = records.len();

    macro_rules! get_max_len {
        ($column_index:expr) => {
            records
                .iter()
                .map(|record| match &record.values[$column_index] {
                    Field::String(value) => value.len(),
                    Field::Text(value) => value.len(),
                    Field::Binary(value) => value.len(),
                    Field::Null => 0,
                    _ => unimplemented!(),
                })
                .max()
                .unwrap()
        };
    }

    macro_rules! column_fields {
        ($index:expr) => {
            records
                .iter_mut()
                .map(move |record| &mut record.values[$index])
        };
    }

    // prepare field values for odbc
    for (i, FieldDefinition { typ, .. }) in dozer_schema.fields.iter().enumerate() {
        match typ {
            FieldType::U128
            | FieldType::I128
            | FieldType::Decimal
            | FieldType::Timestamp
            | FieldType::Date
            | FieldType::Json
            | FieldType::Duration
            | FieldType::Point => {
                for field in column_fields!(i) {
                    match field {
                        Field::U128(value) => *field = Field::String(value.to_string()),
                        Field::I128(value) => *field = Field::String(value.to_string()),
                        Field::Decimal(value) => *field = Field::String(value.to_string()),
                        Field::Timestamp(value) => *field = Field::String(value.to_string()),
                        Field::Date(value) => *field = Field::String(value.to_string()),
                        Field::Json(value) => *field = Field::String(json_to_string(value)),
                        Field::Duration(value) => {
                            *field = Field::String(duration_to_timestamp(value))
                        }
                        Field::Point(value) => *field = Field::Binary(value.to_bytes().to_vec()),
                        _ => (),
                    }
                }
            }
            _ => (),
        }
    }

    let mut schema = Vec::with_capacity(num_columns);
    for (i, FieldDefinition { typ, nullable, .. }) in dozer_schema.fields.iter().enumerate() {
        let nullable = *nullable;
        let desc = match typ {
            FieldType::UInt => BufferDesc::I64 { nullable },
            FieldType::U128 => BufferDesc::Text { max_str_len: 39 },
            FieldType::Int => BufferDesc::I64 { nullable },
            FieldType::I128 => BufferDesc::Text { max_str_len: 40 },
            FieldType::Float => BufferDesc::F64 { nullable },
            FieldType::Boolean => BufferDesc::Bit { nullable },
            FieldType::String => BufferDesc::Text {
                max_str_len: get_max_len!(i),
            },
            FieldType::Text => BufferDesc::Text {
                max_str_len: get_max_len!(i),
            },
            FieldType::Binary => BufferDesc::Binary {
                length: get_max_len!(i),
            },
            FieldType::Decimal => BufferDesc::Text {
                max_str_len: Decimal::MIN.to_string().len(),
            },
            FieldType::Timestamp => BufferDesc::Text { max_str_len: 42 },
            FieldType::Date => BufferDesc::Text { max_str_len: 13 },
            FieldType::Json => BufferDesc::Text {
                max_str_len: get_max_len!(i),
            },
            FieldType::Point => BufferDesc::Binary { length: 16 },
            FieldType::Duration => BufferDesc::Text { max_str_len: 42 },
        };
        schema.push(desc);
    }

    let mut columns = schema
        .iter()
        .map(|desc| AnyBuffer::from_desc(num_rows, *desc))
        .collect::<Vec<_>>();

    for (column_index, column) in columns.iter_mut().enumerate() {
        match column {
            AnyBuffer::Binary(column) => {
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    match field {
                        Field::Binary(bytes) => column.set_value(row_index, Some(bytes)),
                        Field::Null => column.set_value(row_index, None),
                        _ => unreachable!(),
                    }
                }
            }
            AnyBuffer::Text(column) => {
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    let mut set_value =
                        |string: &String| column.set_value(row_index, Some(string.as_bytes()));
                    match field {
                        Field::String(string) => set_value(string),
                        Field::Text(string) => set_value(string),
                        Field::Null => column.set_value(row_index, None),
                        _ => unreachable!(),
                    }
                }
            }
            AnyBuffer::F64(column) => {
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    match field {
                        Field::Float(value) => column[row_index] = value.0,
                        _ => unreachable!(),
                    }
                }
            }
            AnyBuffer::NullableF64(column) => {
                let mut column = column.writer_n(num_rows);
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    match field {
                        Field::Float(value) => column.set_cell(row_index, Some(value.0)),
                        Field::Null => column.set_cell(row_index, None),
                        _ => unreachable!(),
                    }
                }
            }
            AnyBuffer::I64(column) => {
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    match field {
                        Field::Int(value) => column[row_index] = *value,
                        Field::UInt(value) => column[row_index] = *value as i64,
                        _ => unreachable!(),
                    }
                }
            }
            AnyBuffer::NullableI64(column) => {
                let mut column = column.writer_n(num_rows);
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    match field {
                        Field::Int(value) => column.set_cell(row_index, Some(*value)),
                        Field::UInt(value) => column.set_cell(row_index, Some(*value as i64)),
                        Field::Null => column.set_cell(row_index, None),
                        _ => unreachable!(),
                    }
                }
            }
            AnyBuffer::Bit(column) => {
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    match field {
                        Field::Boolean(value) => column[row_index] = Bit::from_bool(*value),
                        _ => unreachable!(),
                    }
                }
            }
            AnyBuffer::NullableBit(column) => {
                let mut column = column.writer_n(num_rows);
                for (row_index, field) in column_fields!(column_index).enumerate() {
                    match field {
                        Field::Boolean(value) => {
                            column.set_cell(row_index, Some(Bit::from_bool(*value)))
                        }
                        Field::Null => column.set_cell(row_index, None),
                        _ => unreachable!(),
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    QueryParams::ColumnarParams {
        schema,
        columns,
        num_rows,
    }
}

fn ident(ident: &str) -> String {
    let identifier_pattern = regex::Regex::new("^[_a-zA-Z][a-zA-Z_0-9$]*$").unwrap();
    let needs_quoting = !identifier_pattern.is_match_at(ident, 0);
    if needs_quoting {
        format!("\"{}\"", ident.replace('"', "\"\""))
    } else {
        ident.to_string()
    }
}

struct ParamBinder {
    columns: Vec<AnyBuffer>,
    num_rows: usize,
}

impl ParamBinder {
    fn new(columns: Vec<AnyBuffer>, num_rows: usize) -> Self {
        Self { columns, num_rows }
    }
}

unsafe impl ParameterCollection for ParamBinder {
    fn parameter_set_size(&self) -> usize {
        self.num_rows
    }

    unsafe fn bind_parameters_to(
        &mut self,
        stmt: &mut impl Statement,
    ) -> Result<(), odbc_api::Error> {
        for (index, column) in self.columns.iter().enumerate() {
            if let Err(error) = stmt
                .bind_input_parameter(index as u16 + 1, column)
                .into_result(stmt)
            {
                stmt.reset_parameters();
                return Err(error);
            }
        }
        Ok(())
    }
}

impl Debug for ParamBinder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParamBinder")
            .field("num_columns", &self.columns.len())
            .field("num_rows", &self.num_rows)
            .finish()
    }
}

pub enum OdbcParam {
    Binary(VarBinaryBox),
    String(VarCharBox),
    Decimal(VarCharBox),
    Timestamp(VarCharBox),
    Date(VarCharBox),
    U8(u8),
    I8(i8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(Bit),
    Null(Nullable<Bit>),
}

macro_rules! odbc_param_dispatch {
    ($odbc_param:ident.$method:ident()) => {
        match $odbc_param {
            Self::Binary(value) => value.$method(),
            Self::String(value) => value.$method(),
            Self::Decimal(value) => value.$method(),
            Self::Timestamp(value) => value.$method(),
            Self::Date(value) => value.$method(),
            Self::U8(value) => value.$method(),
            Self::I8(value) => value.$method(),
            Self::I16(value) => value.$method(),
            Self::U16(value) => value.$method(),
            Self::I32(value) => value.$method(),
            Self::U32(value) => value.$method(),
            Self::I64(value) => value.$method(),
            Self::U64(value) => value.$method(),
            Self::F32(value) => value.$method(),
            Self::F64(value) => value.$method(),
            Self::Bool(value) => value.$method(),
            Self::Null(value) => value.$method(),
        }
    };
}

unsafe impl CData for OdbcParam {
    fn cdata_type(&self) -> odbc_api::sys::CDataType {
        odbc_param_dispatch!(self.cdata_type())
    }

    fn indicator_ptr(&self) -> *const isize {
        odbc_param_dispatch!(self.indicator_ptr())
    }

    fn value_ptr(&self) -> *const std::ffi::c_void {
        odbc_param_dispatch!(self.value_ptr())
    }

    fn buffer_length(&self) -> isize {
        odbc_param_dispatch!(self.buffer_length())
    }
}

unsafe impl CElement for OdbcParam {
    fn assert_completness(&self) {
        odbc_param_dispatch!(self.assert_completness())
    }
}

impl HasDataType for OdbcParam {
    fn data_type(&self) -> odbc_api::DataType {
        match self {
            Self::Binary(_value) => odbc_api::DataType::LongVarbinary {
                length: NonZeroUsize::new(16777216),
            },
            Self::String(_value) => odbc_api::DataType::LongVarchar {
                length: NonZeroUsize::new(16777216),
            },
            Self::Decimal(_value) => odbc_api::DataType::Varchar {
                length: NonZeroUsize::new(255),
            },
            Self::Timestamp(_value) => odbc_api::DataType::Varchar {
                length: NonZeroUsize::new(255),
            },
            Self::Date(_value) => odbc_api::DataType::Varchar {
                length: NonZeroUsize::new(255),
            },
            Self::U8(_value) => odbc_api::DataType::TinyInt,
            Self::I8(value) => value.data_type(),
            Self::I16(value) => value.data_type(),
            Self::U16(_value) => odbc_api::DataType::SmallInt,
            Self::I32(value) => value.data_type(),
            Self::U32(_value) => odbc_api::DataType::Integer,
            Self::I64(value) => value.data_type(),
            Self::U64(_value) => odbc_api::DataType::BigInt,
            Self::F32(value) => value.data_type(),
            Self::F64(value) => value.data_type(),
            Self::Bool(value) => value.data_type(),
            Self::Null(value) => value.data_type(),
        }
    }
}

impl Debug for OdbcParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn varchar_to_string(varchar: &VarCharBox) -> String {
            String::from_utf8_lossy(varchar.as_bytes().unwrap()).into()
        }
        match self {
            Self::Binary(arg0) => f
                .debug_tuple("Binary")
                .field(&arg0.as_bytes().unwrap())
                .finish(),
            Self::String(arg0) => f
                .debug_tuple("String")
                .field(&varchar_to_string(arg0))
                .finish(),
            Self::Decimal(arg0) => f
                .debug_tuple("Decimal")
                .field(&varchar_to_string(arg0))
                .finish(),
            Self::Timestamp(arg0) => f
                .debug_tuple("Timestamp")
                .field(&varchar_to_string(arg0))
                .finish(),
            Self::Date(arg0) => f
                .debug_tuple("Date")
                .field(&varchar_to_string(arg0))
                .finish(),
            Self::U8(arg0) => f.debug_tuple("U8").field(arg0).finish(),
            Self::I8(arg0) => f.debug_tuple("I8").field(arg0).finish(),
            Self::I16(arg0) => f.debug_tuple("I16").field(arg0).finish(),
            Self::U16(arg0) => f.debug_tuple("U16").field(arg0).finish(),
            Self::I32(arg0) => f.debug_tuple("I32").field(arg0).finish(),
            Self::U32(arg0) => f.debug_tuple("U32").field(arg0).finish(),
            Self::I64(arg0) => f.debug_tuple("I64").field(arg0).finish(),
            Self::U64(arg0) => f.debug_tuple("U64").field(arg0).finish(),
            Self::F32(arg0) => f.debug_tuple("F32").field(arg0).finish(),
            Self::F64(arg0) => f.debug_tuple("F64").field(arg0).finish(),
            Self::Bool(arg0) => f.debug_tuple("Bool").field(&arg0.as_bool()).finish(),
            Self::Null(_arg0) => write!(f, "Null"),
        }
    }
}

#[derive(Debug)]
struct ParamVec(Vec<OdbcParam>);

unsafe impl ParameterCollection for ParamVec {
    fn parameter_set_size(&self) -> usize {
        1
    }

    unsafe fn bind_parameters_to(
        &mut self,
        stmt: &mut impl odbc_api::handles::Statement,
    ) -> Result<(), odbc_api::Error> {
        for (index, parameter) in self.0.iter().enumerate() {
            parameter.assert_completness();
            if let Err(error) = stmt
                .bind_input_parameter(index as u16 + 1, parameter)
                .into_result(stmt)
            {
                stmt.reset_parameters();
                return Err(error);
            }
        }
        Ok(())
    }
}
