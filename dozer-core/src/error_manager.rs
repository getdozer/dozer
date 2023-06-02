use std::sync::atomic::AtomicU64;

use dozer_types::{errors::internal::BoxedError, log::error};

/// `ErrorManager` records and counts the number of errors happened.
///
/// It panics when an error threshold is set and reached.
#[derive(Debug)]
pub struct ErrorManager {
    threshold: Option<u64>,
    count: AtomicU64,
}

impl ErrorManager {
    pub fn new_threshold(threshold: u64) -> Self {
        Self {
            threshold: Some(threshold),
            count: AtomicU64::new(0),
        }
    }

    pub fn new_unlimited() -> Self {
        Self {
            threshold: None,
            count: AtomicU64::new(0),
        }
    }

    pub fn report(&self, error: BoxedError) {
        error!("{}", error);
        let count = self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(threshold) = self.threshold {
            if count >= threshold {
                panic!("Error threshold reached: {}", threshold);
            }
        }
    }
}
