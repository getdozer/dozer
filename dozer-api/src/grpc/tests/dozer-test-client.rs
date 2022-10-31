#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Film {
    #[prost(int32, optional, tag="1")]
    pub film_id: ::core::option::Option<i32>,
    #[prost(string, optional, tag="2")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="3")]
    pub rental_rate: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int32, optional, tag="4")]
    pub release_year: ::core::option::Option<i32>,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct SortOptions {
    #[prost(string, tag="1")]
    pub field_name: ::prost::alloc::string::String,
    #[prost(enumeration="sort_options::SortDirection", tag="2")]
    pub direction: i32,
}
/// Nested message and enum types in `SortOptions`.
pub mod sort_options {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SortDirection {
        Asc = 0,
        Desc = 1,
    }
    impl SortDirection {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                SortDirection::Asc => "asc",
                SortDirection::Desc => "desc",
            }
        }
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterExpression {
    #[prost(oneof="filter_expression::Expression", tags="1, 2")]
    pub expression: ::core::option::Option<filter_expression::Expression>,
}
/// Nested message and enum types in `FilterExpression`.
pub mod filter_expression {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Expression {
        #[prost(message, tag="1")]
        Simple(super::SimpleExpression),
        #[prost(message, tag="2")]
        And(super::AndExpression),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleExpression {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration="simple_expression::Operator", tag="2")]
    pub operator: i32,
    #[prost(message, optional, tag="3")]
    pub value: ::core::option::Option<::prost_wkt_types::Value>,
}
/// Nested message and enum types in `SimpleExpression`.
pub mod simple_expression {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Operator {
        Lt = 0,
        Lte = 1,
        Eq = 2,
        Gt = 3,
        Gte = 4,
        Contains = 5,
        MatchesAny = 6,
        MatchesAll = 7,
    }
    impl Operator {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Operator::Lt => "LT",
                Operator::Lte => "LTE",
                Operator::Eq => "EQ",
                Operator::Gt => "GT",
                Operator::Gte => "GTE",
                Operator::Contains => "Contains",
                Operator::MatchesAny => "MatchesAny",
                Operator::MatchesAll => "MatchesAll",
            }
        }
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AndExpression {
    #[prost(message, repeated, tag="1")]
    pub filter_expressions: ::prost::alloc::vec::Vec<FilterExpression>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Event {
    #[prost(enumeration="event::EventType", tag="1")]
    pub r#type: i32,
    #[prost(message, optional, tag="2")]
    pub detail: ::core::option::Option<::prost_wkt_types::Value>,
}
/// Nested message and enum types in `Event`.
pub mod event {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum EventType {
        SchemaChange = 0,
        RecordUpdate = 1,
        RecordInsert = 2,
        RecordDelete = 3,
    }
    impl EventType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                EventType::SchemaChange => "schema_change",
                EventType::RecordUpdate => "record_update",
                EventType::RecordInsert => "record_insert",
                EventType::RecordDelete => "record_delete",
            }
        }
    }
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct GetFilmsRequest {
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct GetFilmsResponse {
    #[prost(message, repeated, tag="1")]
    pub film: ::prost::alloc::vec::Vec<Film>,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct GetFilmsByIdRequest {
    #[prost(int32, tag="1")]
    pub film_id: i32,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct GetFilmsByIdResponse {
    #[prost(message, optional, tag="1")]
    pub film: ::core::option::Option<Film>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryFilmsRequest {
    #[prost(message, optional, tag="1")]
    pub filter: ::core::option::Option<FilterExpression>,
    #[prost(message, repeated, tag="2")]
    pub order_by: ::prost::alloc::vec::Vec<SortOptions>,
    #[prost(uint32, optional, tag="3")]
    pub limit: ::core::option::Option<u32>,
    #[prost(uint32, optional, tag="4")]
    pub skip: ::core::option::Option<u32>,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct QueryFilmsResponse {
    #[prost(message, repeated, tag="1")]
    pub film: ::prost::alloc::vec::Vec<Film>,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct OnChangeRequest {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OnChangeResponse {
    #[prost(message, optional, tag="1")]
    pub event: ::core::option::Option<Event>,
}
/// Generated client implementations.
pub mod films_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct FilmsServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl FilmsServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> FilmsServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> FilmsServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            FilmsServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub async fn films(
            &mut self,
            request: impl tonic::IntoRequest<super::GetFilmsRequest>,
        ) -> Result<tonic::Response<super::GetFilmsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/Dozer.FilmsService/films");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn by_id(
            &mut self,
            request: impl tonic::IntoRequest<super::GetFilmsByIdRequest>,
        ) -> Result<tonic::Response<super::GetFilmsByIdResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/Dozer.FilmsService/by_id");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn query(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryFilmsRequest>,
        ) -> Result<tonic::Response<super::QueryFilmsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/Dozer.FilmsService/query");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn on_change(
            &mut self,
            request: impl tonic::IntoRequest<super::OnChangeRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::OnChangeResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/Dozer.FilmsService/on_change",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
    }
}
