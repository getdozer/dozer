pub mod types;

pub mod storage {
    tonic::include_proto!("storage"); // The string specified here must match the proto package name
}
pub mod ingestion {
    tonic::include_proto!("ingestion");
}
