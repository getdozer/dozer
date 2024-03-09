use dozer_ingestion_connector::dozer_types::thiserror::{self, Error};

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("Unsupported grpc adapter: {0} {1:?}")]
    UnsupportedGrpcAdapter(String, Option<String>),

    #[cfg(feature = "mongodb")]
    #[error("mongodb config error: {0}")]
    MongodbConfig(#[from] dozer_ingestion_mongodb::MongodbConnectorError),

    #[error("mysql config error: {0}")]
    MysqlConfig(#[from] dozer_ingestion_mysql::MySQLConnectorError),

    #[error("postgres config error: {0}")]
    PostgresConfig(#[from] dozer_ingestion_postgres::PostgresConnectorError),

    #[error("snowflake feature is not enabled")]
    SnowflakeFeatureNotEnabled,

    #[error("kafka feature is not enabled")]
    KafkaFeatureNotEnabled,

    #[error("ethereum feature is not enabled")]
    EthereumFeatureNotEnabled,

    #[error("mongodb feature is not enabled")]
    MongodbFeatureNotEnabled,

    #[error("datafusion feature is not enabled")]
    DatafusionFeatureNotEnabled,

    #[error("javascript feature is not enabled")]
    JavascrtiptFeatureNotEnabled,

    #[error("{0} is not supported as a source connector")]
    Unsupported(String),
}
