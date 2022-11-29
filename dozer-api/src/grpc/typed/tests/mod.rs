mod service;
mod utils;

mod generator;
mod test_utils;
// To be used in tests
pub mod types {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("dozer.types");
}

// To be used in tests
pub mod generated {
    pub mod films {
        #![allow(clippy::derive_partial_eq_without_eq)]
        tonic::include_proto!("dozer.generated.films");
    }
}
