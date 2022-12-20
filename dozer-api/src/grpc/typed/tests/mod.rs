pub mod service;
mod test_utils;
mod utils;
// To be used in tests
pub mod types {
    #![allow(clippy::derive_partial_eq_without_eq)]
    #![allow(clippy::enum_variant_names)]
    tonic::include_proto!("dozer.types");
}

// To be used in tests
pub mod generated {
    pub mod films {
        #![allow(clippy::derive_partial_eq_without_eq)]
        #![allow(non_camel_case_types)]
        tonic::include_proto!("dozer.generated.films");
    }
}
