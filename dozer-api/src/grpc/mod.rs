pub mod client_server;
pub mod common;
pub mod dynamic;
pub mod common_grpc {
    tonic::include_proto!("dozer.common"); // The string specified here must match the proto package name
}

pub mod internal_grpc {
    tonic::include_proto!("dozer.internal");
}
