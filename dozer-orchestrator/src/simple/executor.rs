use dozer_api::grpc::internal_grpc::PipelineRequest;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use dozer_api::CacheEndpoint;
use dozer_types::models::source::Source;

use crate::pipeline::{CacheSinkFactory, ConnectorSourceFactory};
use dozer_core::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use dozer_core::dag::errors::ExecutionError::{self};
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::node::NodeHandle;
use dozer_ingestion::connectors::TableInfo;
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};

use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_sql::sqlparser::ast::Statement;
use dozer_sql::sqlparser::dialect::GenericDialect;
use dozer_sql::sqlparser::parser::Parser;
use dozer_types::crossbeam;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;

use crate::errors::OrchestrationError;
use crate::validate;

pub struct Executor {
    sources: Vec<Source>,
    cache_endpoints: Vec<CacheEndpoint>,
    home_dir: PathBuf,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    running: Arc<AtomicBool>,
}
impl Executor {
    pub fn new(
        sources: Vec<Source>,
        cache_endpoints: Vec<CacheEndpoint>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        running: Arc<AtomicBool>,
        home_dir: PathBuf,
    ) -> Self {
        Self {
            sources,
            cache_endpoints,
            home_dir,
            ingestor,
            iterator,
            running,
        }
    }

    pub fn run(
        &self,
        _notifier: Option<crossbeam::channel::Sender<PipelineRequest>>,
        _running: Arc<AtomicBool>,
    ) -> Result<(), OrchestrationError> {
        let mut connections: Vec<Connection> = vec![];
        let mut connection_map: HashMap<String, Vec<TableInfo>> = HashMap::new();
        let mut table_map: Huse crate::ingestion_types::{EthConfig, KafkaConfig, SnowflakeConfig};
        use serde::{
            de::Deserializer,
            ser::{self, Serializer},
        };
        use serde::{Deserialize, Serialize};

        use std::{
            error::Error,
            fmt::{self, Display, Formatter},
            str::FromStr,
        };

        #[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]

        pub struct Connection {
            #[prost(oneof = "Authentication", tags = "1,2,3,4,5")]
            pub authentication: Option<Authentication>,
            #[prost(string, optional, tag = "6")]
            #[serde(skip_serializing_if = "Option::is_none")]
            pub id: Option<String>,
            #[prost(string, optional, tag = "7")]
            #[serde(skip_serializing_if = "Option::is_none")]
            pub app_id: Option<String>,
            #[prost(enumeration = "DBType", tag = "8")]
            #[serde(serialize_with = "serialize_db_type_i32_as_string")]
            #[serde(deserialize_with = "deserialize_db_type_str_as_i32")]
            pub db_type: i32,
            #[prost(string, tag = "9")]
            pub name: String,
        }
        fn serialize_db_type_i32_as_string<S>(input: &i32, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
        {
            let db_type = DBType::try_from(input.to_owned()).map_err(ser::Error::custom)?;
            serializer.serialize_str(db_type.as_str_name())
        }

        fn deserialize_db_type_str_as_i32<'de, D>(deserializer: D) -> Result<i32, D::Error>
            where
                D: Deserializer<'de>,
        {
            let db_type_string = String::deserialize(deserializer)?;
            let db_type = DBType::from_str(&db_type_string).map_err(serde::de::Error::custom)?;
            Ok(db_type as i32)
        }

        #[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Enumeration)]
        #[repr(i32)]
        pub enum DBType {
            Postgres = 0,
            Snowflake = 1,
            Ethereum = 2,
            Events = 3,
            Kafka = 4,
        }
        impl TryFrom<i32> for DBType {
            type Error = Box<dyn Error>;
            fn try_from(item: i32) -> Result<Self, Self::Error> {
                match item {
                    0 => Ok(DBType::Postgres),
                    1 => Ok(DBType::Snowflake),
                    2 => Ok(DBType::Ethereum),
                    3 => Ok(DBType::Events),
                    4 => Ok(DBType::Kafka),
                    _ => Err("DBType enum not match".to_owned())?,
                }
            }
        }

        impl DBType {
            pub fn as_str_name(&self) -> &'static str {
                match self {
                    DBType::Postgres => "postgres",
                    DBType::Snowflake => "snowflake",
                    DBType::Ethereum => "ethereum",
                    DBType::Events => "events",
                    DBType::Kafka => "kafka",
                }
            }
        }
        #[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
        pub struct PostgresAuthentication {
            #[prost(string, tag = "1")]
            pub user: String,
            #[prost(string, tag = "2")]
            pub password: String,
            #[prost(string, tag = "3")]
            pub host: String,
            #[prost(uint32, tag = "4")]
            pub port: u32,
            #[prost(string, tag = "5")]
            pub database: String,
        }
        #[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Message, Hash)]
        pub struct EventsAuthentication {}
        #[derive(Serialize, Deserialize, Eq, PartialEq, Clone, ::prost::Oneof, Hash)]
        pub enum Authentication {
            #[prost(message, tag = "1")]
            Postgres(PostgresAuthentication),
            #[prost(message, tag = "2")]
            Ethereum(EthConfig),
            #[prost(message, tag = "3")]
            Events(EventsAuthentication),
            #[prost(message, tag = "4")]
            Snowflake(SnowflakeConfig),
            #[prost(message, tag = "5")]
            Kafka(KafkaConfig),
        }

        impl Default for Authentication {
            fn default() -> Self {
                Authentication::Postgres(PostgresAuthentication::default())
            }
        }

        impl Display for DBType {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                write!(f, "{:?}", self)
            }
        }

        impl FromStr for DBType {
            type Err = &'static str;
            fn from_str(s: &str) -> Result<DBType, Self::Err> {
                match s {
                    "Postgres" | "postgres" => Ok(DBType::Postgres),
                    "Ethereum" | "ethereum" => Ok(DBType::Ethereum),
                    "Snowflake" | "snowflake" => Ok(DBType::Snowflake),
                    "Kafka" | "kafka" => Ok(DBType::Kafka),
                    "Events" | "events" => Ok(DBType::Events),
                    _ => Err("Not match any value in Enum DBType"),
                }
            }
        }

        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, ::prost::Message)]
        pub struct AuthenticationWrapper {
            #[prost(oneof = "Authentication", tags = "1,2,3,4,5")]
            pub authentication: Option<Authentication>,
        }

        impl From<AuthenticationWrapper> for Authentication {
            fn from(input: AuthenticationWrapper) -> Self {
                input.authentication.unwrap_or_default()
            }
        }
        ashMap<String, u16> = HashMap::new();

        // Initialize Source
        // For every pipeline, there will be one Source implementation
        // that can take multiple Connectors on different ports.
        for (table_id, (idx, source)) in self.sources.iter().cloned().enumerate().enumerate() {
            validate(source.connection.clone())?;

            let table_name = source.table_name.clone();
            let connection = source.connection.to_owned();
            let id = match &connection.id {
                Some(id) => id.clone(),
                None => idx.to_string(),
            };
            connections.push(connection);

            let table = TableInfo {
                name: source.table_name,
                id: table_id as u32,
                columns: source.columns,
            };

            connection_map
                .entry(id)
                .and_modify(|v| v.push(table.clone()))
                .or_insert_with(|| vec![table]);

            table_map.insert(table_name, idx.try_into().unwrap());
        }

        let source_handle = NodeHandle::new(None, "src".to_string());

        let source = ConnectorSourceFactory::new(
            connections,
            connection_map,
            table_map.clone(),
            self.ingestor.to_owned(),
            self.iterator.to_owned(),
        );
        let mut parent_dag = Dag::new();
        parent_dag.add_node(NodeType::Source(Arc::new(source)), source_handle.clone());
        let running_wait = self.running.clone();

        for (idx, cache_endpoint) in self.cache_endpoints.iter().cloned().enumerate() {
            let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

            let api_endpoint = cache_endpoint.endpoint;
            let _api_endpoint_name = api_endpoint.name.clone();
            let cache = cache_endpoint.cache;

            let ast = Parser::parse_sql(&dialect, &api_endpoint.sql).unwrap();

            let statement: &Statement = &ast[0];

            let builder = PipelineBuilder::new(Some(idx as u16));

            let (dag, in_handle, out_handle) = builder
                .statement_to_pipeline(statement.clone())
                .map_err(OrchestrationError::SqlStatementFailed)?;

            parent_dag.merge(Some(idx as u16), dag);

            let sink_handle = NodeHandle::new(Some(idx as u16), "sink".to_string());

            // Initialize Sink
            let sink = CacheSinkFactory::new(vec![DEFAULT_PORT_HANDLE], cache, api_endpoint);
            parent_dag.add_node(NodeType::Sink(Arc::new(sink)), sink_handle.clone());

            // Connect Pipeline to Sink
            parent_dag
                .connect(out_handle, Endpoint::new(sink_handle, DEFAULT_PORT_HANDLE))
                .map_err(OrchestrationError::ExecutionError)?;

            for (table_name, endpoint) in in_handle.into_iter() {
                let port = match table_map.get(&table_name) {
                    Some(port) => Ok(port),
                    None => Err(OrchestrationError::PortNotFound(table_name)),
                }?;

                // Connect source from Parent Dag to Processor
                parent_dag
                    .connect(
                        Endpoint::new(source_handle.clone(), port.to_owned()),
                        endpoint,
                    )
                    .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
            }
        }

        let path = self.home_dir.join("pipeline");
        fs::create_dir_all(&path).map_err(|_e| OrchestrationError::InternalServerError)?;
        let mut exec = DagExecutor::new(&parent_dag, path.as_path(), ExecutorOptions::default())?;

        exec.start()?;
        // Waiting for Ctrl+C
        while running_wait.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(200));
        }

        exec.stop();
        exec.join()
            .map_err(|_e| OrchestrationError::InternalServerError)?;
        Ok(())
    }
}
