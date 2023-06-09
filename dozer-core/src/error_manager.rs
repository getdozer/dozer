use std::sync::atomic::AtomicU32;

use dozer_types::{errors::internal::BoxedError, log::error};

/// `ErrorManager` records and counts the number of errors happened.
///
/// It panics when an error threshold is set and reached.
#[derive(Debug)]
pub struct ErrorManager {
    threshold: Option<u32>,
    count: AtomicU32,
}

impl ErrorManager {
    pub fn new_threshold(threshold: u32) -> Self {
        Self {
            threshold: Some(threshold),
            count: AtomicU32::new(0),
        }
    }

    pub fn new_unlimited() -> Self {
        Self {
            threshold: None,
            count: AtomicU32::new(0),
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
