use std::io::ErrorKind;
use std::sync::Arc;

use ::datafusion::arrow::datatypes::DECIMAL128_MAX_PRECISION;
use ::datafusion::error::DataFusionError;
use async_trait::async_trait;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use dozer_types::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int8Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeStringArray, StringArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use dozer_types::arrow::array::{Int64Array, LargeBinaryArray};
use dozer_types::arrow::datatypes::{DataType, IntervalUnit};
use dozer_types::arrow::datatypes::{TimeUnit, DECIMAL_DEFAULT_SCALE};
use dozer_types::log::{debug, info};
use dozer_types::models::api_config::{default_host, default_sql_port, SqlOptions};
use dozer_types::rust_decimal::Decimal;
use futures_util::future::try_join_all;
use futures_util::stream::BoxStream;
use futures_util::{stream, StreamExt};
use pgwire::api::portal::Portal;
use pgwire::api::stmt::QueryParser;
use pgwire::api::store::MemPortalStore;
use pgwire::messages::data::DataRow;
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{
    DataRowEncoder, DescribeResponse, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::tokio::process_socket;
use tokio::select;

use crate::shutdown::ShutdownReceiver;
use crate::sql::datafusion::SQLExecutor;
use crate::CacheEndpoint;

use super::datafusion::PlannedStatement;
use super::util::Iso8601Duration;

pub struct PgWireServer {
    config: SqlOptions,
}

struct MakeQueryHandler {
    sql_executor: Arc<SQLExecutor>,
}

impl MakeQueryHandler {
    pub async fn try_new(
        cache_endpoints: Vec<Arc<CacheEndpoint>>,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            sql_executor: Arc::new(SQLExecutor::try_new(&cache_endpoints).await?),
        })
    }
}

impl MakeHandler for MakeQueryHandler {
    type Handler = QueryProcessor;

    fn make(&self) -> Self::Handler {
        QueryProcessor::new(self.sql_executor.clone())
    }
}

impl PgWireServer {
    pub fn new(config: SqlOptions) -> Self {
        Self { config }
    }

    pub async fn run(
        &self,
        shutdown: ShutdownReceiver,
        cache_endpoints: Vec<Arc<CacheEndpoint>>,
    ) -> std::io::Result<()> {
        let config = self.config.clone();
        let processor = Arc::new(
            MakeQueryHandler::try_new(cache_endpoints)
                .await
                .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?,
        );
        let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

        let host = config.host.unwrap_or_else(default_host);
        let port = config.port.unwrap_or_else(default_sql_port);
        let server_addr = format!("{}:{}", host, port);
        let listener = TcpListener::bind(&server_addr).await?;
        info!("Starting Postgres Wire Protocol Server on {server_addr}");
        loop {
            select! {
                accept_result = listener.accept() => {
                    let incoming_socket = accept_result?;
                    let authenticator_ref = authenticator.make();
                    let processor_ref = processor.make();
                    let placeholder_ref = processor.make();
                    tokio::spawn(async move {
                        process_socket(
                            incoming_socket.0,
                            None,
                            authenticator_ref,
                            Arc::new(processor_ref),
                            Arc::new(placeholder_ref),
                        )
                        .await
                    });
                }
                _ = shutdown.create_shutdown_future() => break
            }
        }
        Ok(())
    }
}

struct QueryProcessor {
    sql_executor: Arc<SQLExecutor>,
    portal_store: Arc<MemPortalStore<Option<PlannedStatement>>>,
}

impl QueryProcessor {
    pub fn new(sql_executor: Arc<SQLExecutor>) -> Self {
        Self {
            sql_executor,
            portal_store: Arc::new(MemPortalStore::new()),
        }
    }

    async fn execute<'a>(&self, plan: LogicalPlan) -> PgWireResult<Response<'a>> {
        fn error_info(err: DataFusionError) -> Box<ErrorInfo> {
            Box::new(generic_error_info(err.to_string()))
        }

        debug!("Executing query plan {plan:?}");
        let schema = Arc::new(
            plan.schema()
                .fields()
                .iter()
                .map(|field| {
                    let datatype = map_data_type(field.data_type());
                    FieldInfo::new(
                        field.name().clone(),
                        None,
                        None,
                        datatype,
                        FieldFormat::Text,
                    )
                })
                .collect::<Vec<_>>(),
        );
        let dataframe = match self.sql_executor.execute(plan).await {
            Ok(df) => df,
            Err(err) => {
                return Err(PgWireError::UserError(error_info(err)));
            }
        };

        let result = dataframe.execute_stream().await;
        if let Err(err) = result {
            return Err(PgWireError::UserError(error_info(err)));
        }

        let recordbatch_stream = result.unwrap();
        let schema_ref = schema.clone();
        let data_row_stream = recordbatch_stream
            .map(move |recordbatch_result| {
                if let Err(err) = recordbatch_result {
                    return Box::pin(stream::once(async move {
                        Err(PgWireError::UserError(error_info(err)))
                    })) as BoxStream<'_, Result<DataRow, PgWireError>>;
                }
                let recordbatch = recordbatch_result.unwrap();
                let datafusion_schema = recordbatch.schema();
                let pgwire_schema = schema_ref.clone();
                Box::pin(stream::iter((0..recordbatch.num_rows()).map(move |i| {
                    let mut encoder = DataRowEncoder::new(pgwire_schema.clone());
                    for (j, column) in recordbatch.columns().iter().enumerate() {
                        encode_field(
                            &mut encoder,
                            &column,
                            datafusion_schema.fields()[j].data_type(),
                            i,
                        )?
                    }
                    encoder.finish()
                }))) as BoxStream<'_, Result<DataRow, PgWireError>>
            })
            .flatten();

        Ok(Response::Query(QueryResponse::new(schema, data_row_stream)))
    }

    fn pg_schema(&self, stmt: &LogicalPlan) -> Vec<FieldInfo> {
        let schema = stmt.schema();
        schema
            .fields()
            .iter()
            .map(|df_field| {
                let name = df_field.name().to_owned();
                let datatype = map_data_type(df_field.data_type());
                FieldInfo::new(name, None, None, datatype, FieldFormat::Text)
            })
            .collect()
    }
}

#[async_trait]
impl SimpleQueryHandler for QueryProcessor {
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let queries = self
            .sql_executor
            .parse(query)
            .await
            .map_err(|e| PgWireError::UserError(Box::new(generic_error_info(e.to_string()))))?;

        if queries.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        try_join_all(queries.into_iter().map(|q| async {
            match q {
                super::datafusion::PlannedStatement::Query(plan) => self.execute(plan).await,
                super::datafusion::PlannedStatement::Statement(name) => {
                    Ok(Response::Execution(Tag::new_for_execution(name, None)))
                }
            }
        }))
        .await
    }
}

#[async_trait]
impl QueryParser for SQLExecutor {
    type Statement = Option<PlannedStatement>;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let mut plans = self
            .parse(sql)
            .await
            .map_err(|e| PgWireError::UserError(Box::new(generic_error_info(e.to_string()))))?;
        if plans.len() > 1 {
            return Err(PgWireError::UserError(Box::new(generic_error_info(
                "Multiple statements found".to_owned(),
            ))));
        } else if plans.is_empty() {
            Ok(None)
        } else {
            Ok(Some(plans.remove(0)))
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for QueryProcessor {
    type Statement = Option<PlannedStatement>;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = SQLExecutor;

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.sql_executor.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = match portal.statement().statement() {
            Some(PlannedStatement::Query(query)) => query,
            Some(PlannedStatement::Statement(name)) => {
                return Ok(Response::Execution(Tag::new_for_execution(name, None)))
            }
            None => {
                return Ok(Response::EmptyQuery);
            }
        };
        let _params = query.get_parameter_types().map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "FATAL".to_owned(),
                "XXX01".to_owned(),
                e.to_string(),
            )))
        })?;

        let plan = LogicalPlanBuilder::from(query.clone())
            //.limit(0, Some(max_rows))
            //.unwrap()
            .build()
            .unwrap();
        self.execute(plan).await
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        match target {
            StatementOrPortal::Statement(stmt) => {
                let Some(PlannedStatement::Query(query)) = stmt.statement() else {
                    return Ok(DescribeResponse::no_data());
                };
                let unknown_type = Type::new(
                    "unknown".to_owned(),
                    0,
                    postgres_types::Kind::Pseudo,
                    "pg_catalog".to_owned(),
                );
                let types = query.get_parameter_types().map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "XXX01".to_owned(),
                        e.to_string(),
                    )))
                })?;

                let mut types = Vec::from_iter(types);
                // The id's of types are always of the form `$1`, `$2` etc, so
                // a simple sort works
                types.sort();

                let pg_types = types
                    .into_iter()
                    .map(|(_, ty)| {
                        ty.as_ref()
                            .map_or_else(|| unknown_type.clone(), map_data_type)
                    })
                    .collect();
                return Ok(DescribeResponse::new(Some(pg_types), self.pg_schema(query)));
            }
            StatementOrPortal::Portal(portal) => match portal.statement().statement() {
                Some(PlannedStatement::Query(query)) => {
                    Ok(DescribeResponse::new(None, self.pg_schema(query)))
                }
                Some(PlannedStatement::Statement(_)) | None => Ok(DescribeResponse::no_data()),
            },
        }
    }
}

fn map_data_type(datafusion_type: &DataType) -> Type {
    match datafusion_type {
        DataType::Null => Type::BOOL,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::INT2,
        DataType::UInt16 => Type::INT2,
        DataType::UInt32 => Type::INT4,
        DataType::UInt64 => Type::INT8,
        DataType::Float16 => Type::FLOAT4,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Timestamp(_, None) => Type::TIMESTAMP,
        DataType::Timestamp(_, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Date32 => Type::DATE,
        // This might lose data
        DataType::Date64 => Type::DATE,
        DataType::Time32(_) => Type::TIME,
        DataType::Time64(_) => Type::TIME,
        DataType::Duration(_) => Type::INTERVAL,
        DataType::Interval(_) => Type::INTERVAL,
        DataType::Binary => Type::BYTEA,
        DataType::FixedSizeBinary(_) => Type::BYTEA,
        DataType::LargeBinary => Type::BYTEA,
        DataType::Utf8 => Type::VARCHAR,
        DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        DataType::Decimal256(_, _) => Type::NUMERIC,
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            match f.data_type() {
                DataType::Boolean => Type::BOOL_ARRAY,
                DataType::Int8 => Type::CHAR_ARRAY,
                DataType::Int16 => Type::INT2_ARRAY,
                DataType::Int32 => Type::INT4_ARRAY,
                DataType::Int64 => Type::INT8_ARRAY,
                DataType::UInt8 => Type::CHAR_ARRAY,
                DataType::UInt16 => Type::INT2_ARRAY,
                DataType::UInt32 => Type::INT4_ARRAY,
                DataType::UInt64 => Type::INT8_ARRAY,
                DataType::Float16 => Type::FLOAT4_ARRAY,
                DataType::Float32 => Type::FLOAT4_ARRAY,
                DataType::Float64 => Type::FLOAT8_ARRAY,
                DataType::Timestamp(_, _) => Type::TIMESTAMP_ARRAY,
                DataType::Date32 => Type::TIMESTAMPTZ_ARRAY,
                DataType::Date64 => Type::TIMESTAMPTZ_ARRAY,
                DataType::Time32(_) => Type::TIME_ARRAY,
                DataType::Time64(_) => Type::TIME_ARRAY,
                DataType::Duration(_) => Type::INTERVAL_ARRAY,
                DataType::Interval(_) => Type::INTERVAL_ARRAY,
                DataType::Binary => Type::BYTEA_ARRAY,
                DataType::FixedSizeBinary(_) => Type::BYTEA_ARRAY,
                DataType::LargeBinary => Type::BYTEA_ARRAY,
                DataType::Utf8 => Type::VARCHAR_ARRAY,
                DataType::LargeUtf8 => Type::VARCHAR_ARRAY,
                DataType::List(_) => Type::ANYARRAY,
                DataType::FixedSizeList(_, _) => Type::ANYARRAY,
                DataType::LargeList(_) => Type::ANYARRAY,
                DataType::Decimal128(_, _) => Type::NUMERIC_ARRAY,
                DataType::Decimal256(_, _) => Type::NUMERIC_ARRAY,
                _ => unimplemented!(),
            }
        }
        DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => unimplemented!(),
    }
}

macro_rules! cast_array {
    ($array:tt as $type:tt) => {
        $array.as_any().downcast_ref::<$type>().unwrap()
    };
}

fn encode_field(
    encoder: &mut DataRowEncoder,
    column_data: &dyn Array,
    column_data_type: &DataType,
    row_index: usize,
) -> Result<(), PgWireError> {
    if column_data.is_null(row_index) {
        return encoder.encode_field(&None::<bool>);
    }
    match column_data_type {
        DataType::Null => encoder.encode_field(&None::<bool>),
        DataType::Boolean => {
            encoder.encode_field(&cast_array!(column_data as BooleanArray).value(row_index))
        }
        DataType::Int8 => {
            encoder.encode_field(&cast_array!(column_data as Int8Array).value(row_index))
        }
        DataType::Int16 => {
            encoder.encode_field(&cast_array!(column_data as Int16Array).value(row_index))
        }
        DataType::Int32 => {
            encoder.encode_field(&cast_array!(column_data as Int32Array).value(row_index))
        }
        DataType::Int64 => {
            encoder.encode_field(&cast_array!(column_data as Int64Array).value(row_index))
        }
        DataType::UInt8 => encoder.encode_field(
            &cast_array!(column_data as UInt8Array)
                .value(row_index)
                .to_string(),
        ),
        DataType::UInt16 => encoder.encode_field(
            &cast_array!(column_data as UInt16Array)
                .value(row_index)
                .to_string(),
        ),
        DataType::UInt32 => {
            encoder.encode_field(&cast_array!(column_data as UInt32Array).value(row_index))
        }
        DataType::UInt64 => encoder.encode_field(
            &cast_array!(column_data as UInt64Array)
                .value(row_index)
                .to_string(),
        ),
        DataType::Float16 => encoder.encode_field(
            &cast_array!(column_data as Float16Array)
                .value(row_index)
                .to_f32(),
        ),
        DataType::Float32 => {
            encoder.encode_field(&cast_array!(column_data as Float32Array).value(row_index))
        }
        DataType::Float64 => {
            encoder.encode_field(&cast_array!(column_data as Float64Array).value(row_index))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            encoder.encode_field(&cast_array!(column_data as TimestampSecondArray).value(row_index))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => encoder.encode_field(
            &chrono::NaiveDateTime::from_timestamp_millis(
                cast_array!(column_data as TimestampMillisecondArray).value(row_index),
            )
            .unwrap(),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => encoder.encode_field(
            &(chrono::NaiveDateTime::from_timestamp_micros(
                cast_array!(column_data as TimestampMicrosecondArray).value(row_index),
            )
            .unwrap()),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => encoder.encode_field(&{
            let nsecs = cast_array!(column_data as TimestampNanosecondArray).value(row_index);
            let secs = nsecs.div_euclid(1_000_000_000);
            let nsecs = nsecs.rem_euclid(1_000_000_000) as u32;
            chrono::NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap()
        }),
        DataType::Date32 => encoder.encode_field(
            &chrono::NaiveDate::from_ymd_opt(
                0,
                0,
                cast_array!(column_data as Date32Array).value(row_index) as u32,
            )
            .unwrap(),
        ),
        DataType::Date64 => encoder.encode_field(
            &chrono::NaiveDateTime::from_timestamp_millis(
                cast_array!(column_data as Date64Array).value(row_index),
            )
            .unwrap()
            .date(),
        ),
        DataType::Time32(TimeUnit::Second) => encoder.encode_field(
            &chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                cast_array!(column_data as Time32SecondArray).value(row_index) as u32,
                0,
            )
            .unwrap(),
        ),
        DataType::Time32(TimeUnit::Millisecond) => {
            encoder.encode_field(&chrono::NaiveTime::from_hms_milli_opt(
                0,
                0,
                0,
                cast_array!(column_data as Time32MillisecondArray).value(row_index) as u32,
            ))
        }
        DataType::Time64(TimeUnit::Microsecond) => encoder.encode_field({
            let micros = cast_array!(column_data as Time64MicrosecondArray).value(row_index);
            let secs = micros.div_euclid(1_000_000) as u32;
            let micros = micros.rem_euclid(1_000_000) as u32;
            &chrono::NaiveTime::from_hms_micro_opt(0, 0, secs, micros)
        }),
        DataType::Time64(TimeUnit::Nanosecond) => encoder.encode_field({
            let nanos = cast_array!(column_data as Time64NanosecondArray).value(row_index);
            let secs = nanos.div_euclid(1_000_000_000) as u32;
            let nanos = nanos.rem_euclid(1_000_000_000) as u32;
            &chrono::NaiveTime::from_hms_nano_opt(0, 0, secs, nanos)
        }),
        DataType::Duration(TimeUnit::Second) => encoder.encode_field({
            let secs = cast_array!(column_data as DurationSecondArray).value(row_index);
            &Iso8601Duration::Duration(chrono::Duration::seconds(secs)).to_string()
        }),
        DataType::Duration(TimeUnit::Millisecond) => encoder.encode_field({
            let millis = cast_array!(column_data as DurationMillisecondArray).value(row_index);
            &Iso8601Duration::Duration(chrono::Duration::milliseconds(millis)).to_string()
        }),
        DataType::Duration(TimeUnit::Microsecond) => encoder.encode_field({
            let micros = cast_array!(column_data as DurationMicrosecondArray).value(row_index);
            &Iso8601Duration::Duration(chrono::Duration::microseconds(micros)).to_string()
        }),
        DataType::Duration(TimeUnit::Nanosecond) => encoder.encode_field({
            let nanos = cast_array!(column_data as DurationNanosecondArray).value(row_index);
            &Iso8601Duration::Duration(chrono::Duration::nanoseconds(nanos)).to_string()
        }),
        DataType::Interval(IntervalUnit::DayTime) => encoder.encode_field({
            let value = cast_array!(column_data as IntervalDayTimeArray).value(row_index);
            let (days, milliseconds) = (value as i32, (value >> 32) as i32);
            &Iso8601Duration::DaysMilliseconds(days, milliseconds).to_string()
        }),
        DataType::Interval(IntervalUnit::MonthDayNano) => encoder.encode_field({
            let value = cast_array!(column_data as IntervalMonthDayNanoArray).value(row_index);
            let (months, days, nanoseconds) =
                (value as i32, (value >> 32) as i32, (value >> 64) as i64);
            &Iso8601Duration::MonthsDaysNanoseconds(months, days, nanoseconds).to_string()
        }),
        DataType::Interval(IntervalUnit::YearMonth) => encoder.encode_field({
            let months = cast_array!(column_data as IntervalYearMonthArray).value(row_index);
            &Iso8601Duration::Months(months).to_string()
        }),
        DataType::Binary => {
            encoder.encode_field(&cast_array!(column_data as BinaryArray).value(row_index))
        }
        DataType::FixedSizeBinary(_) => {
            encoder.encode_field(&cast_array!(column_data as FixedSizeBinaryArray).value(row_index))
        }
        DataType::LargeBinary => {
            encoder.encode_field(&cast_array!(column_data as LargeBinaryArray).value(row_index))
        }
        DataType::Utf8 => {
            encoder.encode_field(&cast_array!(column_data as StringArray).value(row_index))
        }
        DataType::LargeUtf8 => {
            encoder.encode_field(&cast_array!(column_data as LargeStringArray).value(row_index))
        }
        DataType::Decimal128(_, scale) => encoder.encode_field({
            let value = cast_array!(column_data as Decimal128Array).value(row_index);
            &Decimal::from_i128_with_scale(value, *scale as u32).to_string()
        }),
        DataType::Decimal256(_, _) => encoder.encode_field({
            let array = cast_array!(column_data as Decimal256Array).slice(row_index, 1);
            let precision = DECIMAL128_MAX_PRECISION;
            let scale = DECIMAL_DEFAULT_SCALE;
            let array =
                dozer_types::arrow_cast::cast(&array, &DataType::Decimal128(precision, scale))
                    .map_err(|err| {
                        PgWireError::UserError(Box::new(generic_error_info(err.to_string())))
                    })?;
            let value = cast_array!(array as Decimal128Array).value(0);
            &Decimal::from_i128_with_scale(value, scale as u32).to_string()
        }),

        DataType::List(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => unimplemented!(),

        DataType::Time32(TimeUnit::Microsecond)
        | DataType::Time32(TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second)
        | DataType::Time64(TimeUnit::Millisecond) => unreachable!(),
    }
}

fn generic_error_info(err: String) -> ErrorInfo {
    ErrorInfo::new("ERROR".to_string(), "2F000".to_string(), err)
}
