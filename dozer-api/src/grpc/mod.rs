mod client_server;
pub mod common;
pub mod dynamic;
mod internal_server;
pub mod types_helper;
pub mod types {
    tonic::include_proto!("dozer.types"); // The string specified here must match the proto package name
}

pub mod common_grpc {
    tonic::include_proto!("dozer.common"); // The string specified here must match the proto package name
}

pub mod internal_grpc {
    tonic::include_proto!("dozer.internal");
}

pub use client_server::ApiServer;
pub use internal_server::{start_internal_client, start_internal_server, InternalServer};
