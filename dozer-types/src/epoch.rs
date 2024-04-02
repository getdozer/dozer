use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, NaiveDateTime};

use crate::node::SourceStates;

#[derive(Clone, Debug)]
pub struct EpochCommonInfo {
    pub id: u64,
    pub source_states: Arc<SourceStates>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct SourceTime {
    millis_since_epoch: u64,
    accuracy: u64,
}

impl SourceTime {
    pub fn elapsed_millis(&self) -> Option<u64> {
        let now_duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before 1970-01-01 00:00");
        let rounded_now_millis: u64 = now_duration
            .as_millis()
            .next_multiple_of(self.accuracy.into())
            .try_into()
            // Won't overflow a u64 in the coming 500 million years
            .unwrap();
        rounded_now_millis.checked_sub(self.millis_since_epoch)
    }

    pub fn from_chrono<Tz: chrono::TimeZone>(date: &DateTime<Tz>, accuracy: u64) -> Self {
        Self {
            millis_since_epoch: date
                .naive_utc()
                .signed_duration_since(NaiveDateTime::UNIX_EPOCH)
                .num_milliseconds()
                .try_into()
                .expect("Only source times after 1970-01-01 00:00 are supported"),
            accuracy,
        }
    }

    pub fn new(millis_since_epoch: u64, accuracy: u64) -> Self {
        Self {
            millis_since_epoch,
            accuracy,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Epoch {
    pub common_info: EpochCommonInfo,
    pub decision_instant: SystemTime,
    pub source_time: Option<SourceTime>,
}

impl Epoch {
    pub fn new(id: u64, source_states: Arc<SourceStates>, decision_instant: SystemTime) -> Self {
        Self {
            common_info: EpochCommonInfo { id, source_states },
            decision_instant,
            source_time: None,
        }
    }

    pub fn with_source_time(mut self, source_time: SourceTime) -> Self {
        self.source_time = Some(source_time);
        self
    }
}
