#![allow(clippy::all)]
#![allow(non_camel_case_types)]
pub mod common_grpc {
    tonic::include_proto!("dozer.common"); // The string specified here must match the proto package name
}

mod service;
pub use service::ApiService;
mod helper;
