use std::path::{Path, PathBuf};
use std::sync::Arc;

use lmdb::{Database, DatabaseFlags, Environment};
use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, Recorder, Unit};
use metrics::{KeyName, SharedString};

struct DbHandle {
    env: Environment,
    db: Database,
}
struct MetricsHandle {
    key: Key,
    db_handle: Arc<DbHandle>,
}

impl CounterFn for MetricsHandle {
    fn increment(&self, value: u64) {
        println!("counter increment for '{}': {}", self.key, value);
    }

    fn absolute(&self, value: u64) {
        println!("counter absolute for '{}': {}", self.key, value);
    }
}

impl GaugeFn for MetricsHandle {
    fn increment(&self, value: f64) {
        println!("gauge increment for '{}': {}", self.key, value);
    }

    fn decrement(&self, value: f64) {
        println!("gauge decrement for '{}': {}", self.key, value);
    }

    fn set(&self, value: f64) {
        println!("gauge set for '{}': {}", self.key, value);
    }
}

impl HistogramFn for MetricsHandle {
    fn record(&self, value: f64) {
        println!("histogram record for '{}': {}", self.key, value);
    }
}

pub struct MetricsRecorder {
    db_handle: Arc<DbHandle>,
}

impl MetricsRecorder {
    pub fn new(path: PathBuf) -> Self {
        let db_handle = init_env(path);
        Self {
            db_handle: Arc::new(db_handle),
        }
    }
}

impl Recorder for MetricsRecorder {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        println!(
            "(counter) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        println!(
            "(gauge) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        println!(
            "(histogram) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn register_counter(&self, key: &Key) -> Counter {
        Counter::from_arc(Arc::new(MetricsHandle {
            key: key.clone(),
            db_handle: self.db_handle.clone(),
        }))
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        Gauge::from_arc(Arc::new(MetricsHandle {
            key: key.clone(),
            db_handle: self.db_handle.clone(),
        }))
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        Histogram::from_arc(Arc::new(MetricsHandle {
            key: key.clone(),
            db_handle: self.db_handle.clone(),
        }))
    }
}

fn init_env(path: PathBuf) -> DbHandle {
    let env = Environment::new()
        .open(Path::new(&path))
        .expect("Unable to initialize lmdb env for metrics");

    let db = env
        .create_db(None, DatabaseFlags::default())
        .expect("Unable to create lmdb database for metrics");
    DbHandle { env, db }
}
