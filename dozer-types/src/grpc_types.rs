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

pub mod admin {
    #![allow(clippy::derive_partial_eq_without_eq, clippy::large_enum_variant)]
    tonic::include_proto!("dozer.admin");
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("admin");
}

// To be used in tests
pub mod generated {
    pub mod films {
        #![allow(clippy::derive_partial_eq_without_eq)]
        #![allow(non_camel_case_types)]
        tonic::include_proto!("dozer.generated.films");
    }
}
