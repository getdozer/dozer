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
    #[prost(enumeration="sort_options::SortDirection", tag="3")]
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
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct GetFilmsRequest {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFilmsResponse {
    #[prost(message, repeated, tag="1")]
    pub film: ::prost::alloc::vec::Vec<Film>,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct GetFilmsByIdRequest {
    #[prost(int32, tag="1")]
    pub film_id: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFilmsByIdResponse {
    #[prost(message, optional, tag="1")]
    pub film: ::core::option::Option<Film>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryFilmsRequest {
    #[prost(string, optional, tag="1")]
    pub filter: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, repeated, tag="2")]
    pub order_by: ::prost::alloc::vec::Vec<SortOptions>,
    #[prost(uint32, optional, tag="3")]
    pub limit: ::core::option::Option<u32>,
    #[prost(uint32, optional, tag="4")]
    pub skip: ::core::option::Option<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryFilmsResponse {
    #[prost(message, repeated, tag="1")]
    pub film: ::prost::alloc::vec::Vec<Film>,
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
            let path = http::uri::PathAndQuery::from_static(
                "/DozerFilms.FilmsService/films",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn films_by_id(
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
            let path = http::uri::PathAndQuery::from_static(
                "/DozerFilms.FilmsService/films_by_id",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn query_films(
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
            let path = http::uri::PathAndQuery::from_static(
                "/DozerFilms.FilmsService/query_films",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
