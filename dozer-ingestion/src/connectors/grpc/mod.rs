#[allow(dead_code)]
pub mod connector;
mod ingest;
pub mod ingest_grpc {
    tonic::include_proto!("dozer.ingest");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("ingest");
}
pub mod types {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.types");
}
