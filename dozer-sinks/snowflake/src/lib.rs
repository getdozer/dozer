use std::{thread, time::Duration, usize};

use dozer_api::shutdown::ShutdownReceiver;
use dozer_ingestion_snowflake::{
    connection::{client::Client, params::OdbcValue},
    SnowflakeError,
};
use dozer_log::{
    reader::LogClient,
    replication::LogOperation,
    schemas::EndpointSchema,
    tokio::{self, select},
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
    types::{Field, FieldType, Record, Schema},
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
                select! {
                    result = $future => result,
                    _ = shutdown.create_shutdown_future() => return,
                }
            };
        }

        let endpoint = &self.config.endpoint;
        let LogConnection {
            mut log_client,
            endpoint_schema,
            mut server_id,
        } = retry_internal_pipeline_connect(self.app_server_url.clone(), endpoint.clone()).await;

        let (query_sender, query_receiver) = channel(1);
        let (result_sender, mut result_receiver) = channel(1);
        thread::spawn({
            let config = self.config.connection.clone();
            || snowflake_client(query_receiver, result_sender, config)
        });

        macro_rules! execute_queries {
            ($queries:expr) => {{
                let queries = $queries;
                query_sender
                    .send(queries.clone())
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
        let timeout_in_millis = 300;
        let suspend_warehouse = options
            .suspend_warehouse_after_each_batch
            .unwrap_or_else(default_suspend_warehouse);

        let mut current_end = 0;
        let mut processed_ops = 0;
        loop {
            let request = LogRequest {
                endpoint: endpoint.clone(),
                start: current_end,
                end: current_end + batch_size as u64,
                timeout_in_millis,
            };
            let result = try_await!(log_client.get_log(request));

            let ops = match result {
                Ok(ops) => ops,
                Err(err) => {
                    error!("log read error: {err}");
                    let connection = retry_internal_pipeline_connect(
                        self.app_server_url.clone(),
                        endpoint.clone(),
                    )
                    .await;
                    log_client = connection.log_client;
                    if server_id != connection.server_id {
                        server_id = connection.server_id;
                        info!("Pipeline was restarted from scratch. The Snowflake sink for endpoint {endpoint} will restart as well.");
                        // reset offset
                        current_end = 0;
                        // recreate table
                        execute_queries!(self.ddl_queries(&schema));
                    }
                    continue;
                }
            };

            if ops.is_empty() {
                debug!(
                    "No more pending log operations. Sleeping for {} seconds.",
                    batch_interval.as_secs()
                );
                if suspend_warehouse && processed_ops > 0 {
                    execute_queries!(vec![format!(
                        "ALTER WAREHOUSE {} SUSPEND",
                        ident(&self.config.connection.warehouse)
                    )]);
                }
                try_await!(tokio::time::sleep(batch_interval));
                processed_ops = 0;
                continue;
            }

            let next_end = current_end + ops.len() as u64;

            let ops = ops
                .into_iter()
                .filter(|op| matches!(op, LogOperation::Op { .. }))
                .collect::<Vec<_>>();

            debug!(
                "Applying {} log operations for endpoint {endpoint} to Snowflake sink",
                ops.len()
            );
            let dml_queries = self.operations_to_dml(&schema, &ops);
            execute_queries!(dml_queries);

            debug!(
                "Applied {} log operations to Snowflake sink for endpoint {endpoint}",
                ops.len()
            );

            processed_ops += next_end - current_end;
            current_end = next_end;
        }
    }

    fn ddl_queries(&self, endpoint_schema: &Schema) -> Vec<String> {
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
    }

    fn operations_to_dml(
        &self,
        endpoint_schema: &Schema,
        operations: &[LogOperation],
    ) -> Vec<String> {
        struct QueryBuilder<'a> {
            schema: &'a Schema,
            pub queries: Vec<String>,
        }

        impl<'a> QueryBuilder<'a> {
            fn new(schema: &'a Schema) -> Self {
                Self {
                    schema,
                    queries: Vec::new(),
                }
            }

            fn where_clause(&mut self, record: &Record) -> String {
                let fields = record.values.iter().enumerate();
                if !self.schema.primary_index.is_empty() {
                    fields
                        .filter(|(i, _)| self.schema.primary_index.contains(i))
                        .map(|(i, f)| self.format_kv(i, f))
                        .collect::<Vec<_>>()
                } else {
                    fields
                        .map(|(i, f)| self.format_kv(i, f))
                        .collect::<Vec<_>>()
                }
                .join(", ")
            }

            fn format_kv(&mut self, field_index: usize, field_value: &Field) -> String {
                format!(
                    "{} = {}",
                    self.schema.fields[field_index].name,
                    field_to_query_param(field_value).as_sql()
                )
            }

            pub fn delete(&mut self, table: &str, records: &[&Record]) {
                debug_assert_eq!(records.len(), 1);
                let query = format!(
                    "DELETE FROM {table} WHERE {}",
                    if records.len() == 1 {
                        self.where_clause(records[0])
                    } else {
                        records
                            .iter()
                            .map(|record| format!("({})", self.where_clause(record)))
                            .collect::<Vec<_>>()
                            .join(" OR ")
                    }
                );
                self.queries.push(query);
            }

            pub fn insert(&mut self, table: &str, records: &[&Record]) {
                let query = format!(
                    "INSERT INTO {table}({}) VALUES{}",
                    self.schema
                        .fields
                        .iter()
                        .map(|f| f.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    {
                        records
                            .iter()
                            .map(|record| {
                                format!(
                                    "({})",
                                    record
                                        .values
                                        .iter()
                                        .map(|v| field_to_query_param(v).as_sql())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                );
                self.queries.push(query);
            }

            pub fn update(&mut self, table: &str, old: &Record, new: &Record) {
                let query = format!(
                    "UPDATE {table} SET {} WHERE {}",
                    new.values
                        .iter()
                        .enumerate()
                        .map(|(i, f)| self.format_kv(i, f))
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.where_clause(old)
                );
                self.queries.push(query);
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
                        query_builder.insert(&table, &insert);
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
                    }
                }
                LogOperation::Commit { .. } | LogOperation::SnapshottingDone { .. } => {
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

fn snowflake_client(
    mut query_receiver: Receiver<Vec<String>>,
    result_sender: Sender<Result<(), SnowflakeError>>,
    config: snowflake::ConnectionParameters,
) {
    let env = odbc::create_environment_v3_with_os_db_encoding("utf8", "utf8").unwrap();
    let client = Client::new(config.into(), &env);
    client
        .exec("alter session set MULTI_STATEMENT_COUNT = 0")
        .expect("failed to setup snowflake ODBC session");

    loop {
        let Some(sql_queries) = query_receiver.blocking_recv() else {
            break;
        };
        let query = {
            if sql_queries.len() == 1 {
                sql_queries.into_iter().last().unwrap()
            } else {
                format!("BEGIN; {}; COMMIT", sql_queries.join("; "))
            }
        };
        let result = client.exec(&query);

        if let Err(err) = &result {
            error!("Error executing query {query:?}: {err:?}")
        } else {
            debug!("Executed query {query:?}");
        }

        let Ok(()) = result_sender.blocking_send(result) else {
            break;
        };
    }
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

fn field_to_query_param(field: &Field) -> OdbcValue {
    match field {
        Field::UInt(u) => OdbcValue::U64(*u),
        Field::U128(u) => OdbcValue::String(u.to_string()),
        Field::Int(i) => OdbcValue::I64(*i),
        Field::I128(i) => OdbcValue::String(i.to_string()),
        Field::Float(f) => OdbcValue::F64(f.0),
        Field::Boolean(b) => OdbcValue::Bool(*b),
        Field::String(s) => OdbcValue::String(s.clone()),
        Field::Text(s) => OdbcValue::String(s.clone()),
        Field::Binary(b) => OdbcValue::Binary(b.clone()),
        Field::Decimal(d) => OdbcValue::Decimal(d.to_string()),
        Field::Timestamp(t) => OdbcValue::Timestamp(t.to_string()),
        Field::Date(d) => OdbcValue::Date(d.to_string()),
        Field::Json(j) => OdbcValue::String(json_to_string(j)),
        Field::Point(p) => OdbcValue::Binary(p.to_bytes().to_vec()),
        Field::Duration(d) => OdbcValue::Timestamp(
            ("0001-01-01T00:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
                + d.0)
                .to_string(),
        ),
        Field::Null => OdbcValue::Null,
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
