pub mod types {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.types"); // The string specified here must match the proto package name
}

pub mod common {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.common"); // The string specified here must match the proto package name
}
pub mod health {
    #![allow(non_camel_case_types)]
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.health"); // The string specified here must match the proto package name
}
pub mod internal {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.internal");
}

pub mod auth {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.auth");
}

pub mod ingest {
    tonic::include_proto!("dozer.ingest");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("ingest");
}

pub mod cloud {
    #![allow(clippy::derive_partial_eq_without_eq, clippy::large_enum_variant)]
    tonic::include_proto!("dozer.cloud");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("cloud");
    use crate::chrono::NaiveDateTime;
    use prost_types::Timestamp;
    pub fn naive_datetime_to_timestamp(naive_dt: NaiveDateTime) -> Timestamp {
        let unix_timestamp = naive_dt.timestamp(); // Get the UNIX timestamp (seconds since epoch)
        let nanos = naive_dt.timestamp_subsec_nanos() as i32; // Get nanoseconds part
        prost_types::Timestamp {
            seconds: unix_timestamp,
            nanos,
        }
    }
}

pub mod live {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.live");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("live");
}

// To be used in tests
pub mod generated {
    pub mod films {
        #![allow(clippy::derive_partial_eq_without_eq)]
        #![allow(non_camel_case_types)]
        tonic::include_proto!("dozer.generated.films");
    }
}
