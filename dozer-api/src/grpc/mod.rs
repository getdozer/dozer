mod client_server;
pub mod common;
pub mod internal;
// pub mod dynamic;
mod auth_middleware;
mod shared_impl;
pub mod typed;
pub mod types_helper;
pub mod types {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.types"); // The string specified here must match the proto package name
}

pub mod common_grpc {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.common"); // The string specified here must match the proto package name
}
pub mod internal_grpc {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.internal");
}
pub use client_server::ApiServer;
