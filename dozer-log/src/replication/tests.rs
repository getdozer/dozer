use std::{sync::Arc, time::Duration};

use dozer_types::parking_lot::Mutex;
use tempdir::TempDir;
use tokio::runtime::Runtime;

use crate::{
    replication::{Log, LogOperation, LogResponse},
    storage::{create_temp_dir_local_storage, Queue},
};

async fn create_test_log() -> (TempDir, Arc<Mutex<Log>>, Queue) {
    let (temp_dir, storage) = create_temp_dir_local_storage().await;
    let log = Log::new(&*storage, "log".to_string(), false).await.unwrap();
    let queue = Queue::new(storage, 10).0;
    (temp_dir, Arc::new(Mutex::new(log)), queue)
}

fn create_runtime() -> Arc<Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .into()
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn write_read() {
    let (_temp_dir, log, _) = create_test_log().await;

    let ops = vec![
        LogOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];

    let mut log_mut = log.lock();
    for op in &ops {
        log_mut.write(op.clone());
    }

    let range = 1..ops.len();
    let ops_read_future = log_mut.read(range.clone(), Duration::from_secs(1), log.clone());
    drop(log_mut);
    let ops_read = ops_read_future.await.unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn watch_write() {
    let (_temp_dir, log, _) = create_test_log().await;

    let range = 1..3;
    let mut log_mut = log.lock();
    let handle = tokio::spawn(log_mut.read(range.clone(), Duration::from_secs(1), log.clone()));

    let ops = vec![
        LogOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];
    for op in &ops {
        log_mut.write(op.clone());
    }
    drop(log_mut);

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[allow(clippy::async_yields_async)]
#[test]
fn watch_partial() {
    let runtime = create_runtime();

    let (_temp_dir, log, queue) = runtime.block_on(create_test_log());

    let mut log_mut = log.lock();
    let future =
        runtime.block_on(async { log_mut.read(1..3, Duration::from_secs(1), log.clone()) });
    let handle = runtime.spawn(future);

    let ops = vec![
        LogOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
    ];
    for op in &ops {
        log_mut.write(op.clone());
    }
    drop(log_mut);
    // Persist must be called outside of tokio runtime.
    let runtime_clone = runtime.clone();
    std::thread::spawn(move || {
        log.lock()
            .persist(&queue, log.clone(), &runtime_clone)
            .unwrap();
    })
    .join()
    .unwrap();

    let ops_read = runtime.block_on(handle).unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[1..].to_vec()));
}

#[allow(clippy::async_yields_async)]
#[test]
fn watch_out_of_range() {
    let runtime = create_runtime();

    let (_temp_dir, log, queue) = runtime.block_on(create_test_log());

    let range = 2..3;
    let mut log_mut = log.lock();
    let future = runtime
        .block_on(async { log_mut.read(range.clone(), Duration::from_secs(1), log.clone()) });
    let handle = runtime.spawn(future);

    let ops = vec![
        LogOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];
    for op in &ops[0..2] {
        log_mut.write(op.clone());
    }
    drop(log_mut);

    let log_clone = log.clone();
    let runtime_clone = runtime.clone();
    std::thread::spawn(move || {
        log_clone
            .lock()
            .persist(&queue, log_clone.clone(), &runtime_clone)
            .unwrap();
    })
    .join()
    .unwrap();

    log.lock().write(ops[2].clone());

    let ops_read = runtime.block_on(handle).unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[test]
fn in_memory_log_should_shrink_after_persist() {
    let runtime = create_runtime();

    let (_temp_dir, log, queue) = runtime.block_on(create_test_log());

    let ops = vec![
        LogOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];
    let mut log_mut = log.lock();
    log_mut.write(ops[0].clone());
    log_mut.write(ops[1].clone());
    drop(log_mut);

    let log_clone = log.clone();
    let runtime_clone = runtime.clone();
    let handle = std::thread::spawn(move || {
        log_clone
            .lock()
            .persist(&queue, log_clone.clone(), &runtime_clone)
            .unwrap()
    })
    .join()
    .unwrap();

    log.lock().write(ops[2].clone());
    runtime.block_on(handle).unwrap().unwrap();
    assert!(matches!(
        runtime
            .block_on(log.lock().read(0..1, Duration::from_secs(1), log.clone()))
            .unwrap(),
        LogResponse::Persisted(_)
    ));
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn watch_partial_timeout() {
    let (_temp_dir, log, _) = create_test_log().await;

    let mut log_mut = log.lock();
    let handle = tokio::spawn(log_mut.read(0..2, Duration::from_secs(0), log.clone()));

    let op = LogOperation::SnapshottingDone {
        connection_name: "0".to_string(),
    };
    log_mut.write(op.clone());
    drop(log_mut);

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(vec![op]));
}

#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn write_watch_partial_timeout() {
    let (_temp_dir, log, _) = create_test_log().await;

    let op = LogOperation::SnapshottingDone {
        connection_name: "0".to_string(),
    };
    let mut log_mut = log.lock();
    log_mut.write(op.clone());

    let ops_read_future = log_mut.read(0..2, Duration::from_secs(0), log.clone());
    drop(log_mut);
    let ops_read = ops_read_future.await.unwrap();
    assert_eq!(ops_read, LogResponse::Operations(vec![op]));
}
