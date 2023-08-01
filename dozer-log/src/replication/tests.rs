use std::{sync::Arc, time::Duration};

use dozer_types::models::app_config::LogStorage;
use tempdir::TempDir;
use tokio::sync::Mutex;

use crate::{
    home_dir::{BuildId, HomeDir},
    replication::{Log, LogOperation, LogResponse},
};

use super::LogOptions;

async fn create_test_log(
    temp_dir_prefix: &str,
    entry_max_size: usize,
) -> (TempDir, Arc<Mutex<Log>>) {
    let temp_dir = TempDir::new(temp_dir_prefix).unwrap();
    let home_dir = HomeDir::new(temp_dir.path().to_str().unwrap(), String::default()).unwrap(); // We don't care about the cache dir.
    let build_path = home_dir
        .create_build_dir_all("endpoint", BuildId::first())
        .unwrap();
    let log = Log::new(
        LogOptions {
            storage_config: LogStorage::Local(()),
            max_num_immutable_entries: 10,
            entry_max_size,
        },
        &build_path,
        false,
    )
    .await
    .unwrap();
    (temp_dir, Arc::new(Mutex::new(log)))
}

#[tokio::test]
async fn write_read_mutable() {
    let (_temp_dir, log) = create_test_log("write_read_mutable", 10).await;

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

    let mut log_mut = log.lock().await;
    for op in &ops {
        log_mut.write(op.clone(), log.clone()).await.unwrap();
    }

    let range = 1..ops.len();
    let ops_read = log_mut
        .read(range.clone(), Duration::from_secs(1), log.clone())
        .await
        .unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[tokio::test]
async fn watch_write_mutable() {
    let (_temp_dir, log) = create_test_log("watch_write_mutable", 10).await;

    let range = 1..3;
    let mut log_mut = log.lock().await;
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
        log_mut.write(op.clone(), log.clone()).await.unwrap();
    }
    drop(log_mut);

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[tokio::test]
async fn watch_partial_write_mutable() {
    let (_temp_dir, log) = create_test_log("watch_partial_write_mutable", 2).await;

    let mut log_mut = log.lock().await;
    let handle = tokio::spawn(log_mut.read(1..3, Duration::from_secs(1), log.clone()));

    let ops = vec![
        LogOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        LogOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
    ];
    for op in &ops {
        log_mut.write(op.clone(), log.clone()).await.unwrap();
    }
    drop(log_mut);

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[1..].to_vec()));
}

#[tokio::test]
async fn watch_out_of_range_write_mutable() {
    let (_temp_dir, log) = create_test_log("watch_out_of_range_write_mutable", 2).await;

    let range = 2..3;
    let mut log_mut = log.lock().await;
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
        log_mut.write(op.clone(), log.clone()).await.unwrap();
    }
    drop(log_mut);

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[tokio::test]
async fn in_memory_log_should_shrink() {
    let (_temp_dir, log) = create_test_log("in_memory_log_should_shrink", 2).await;

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
    let mut log_mut = log.lock().await;
    assert!(log_mut
        .write(ops[0].clone(), log.clone())
        .await
        .unwrap()
        .is_none());
    let handle = log_mut
        .write(ops[1].clone(), log.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(log_mut
        .write(ops[2].clone(), log.clone())
        .await
        .unwrap()
        .is_none());
    drop(log_mut);
    handle.await.unwrap();
    assert!(matches!(
        log.lock()
            .await
            .read(0..1, Duration::from_secs(1), log.clone())
            .await
            .unwrap(),
        LogResponse::Persisted(_)
    ));
}

#[tokio::test]
async fn watch_partial_timeout() {
    let (_temp_dir, log) = create_test_log("watch_partial_timeout", 10).await;

    let mut log_mut = log.lock().await;
    let handle = tokio::spawn(log_mut.read(0..2, Duration::from_secs(0), log.clone()));

    let op = LogOperation::SnapshottingDone {
        connection_name: "0".to_string(),
    };
    log_mut.write(op.clone(), log.clone()).await.unwrap();
    drop(log_mut);

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(vec![op]));
}

#[tokio::test]
async fn write_watch_partial_timeout() {
    let (_temp_dir, log) = create_test_log("write_watch_partial_timeout", 10).await;

    let op = LogOperation::SnapshottingDone {
        connection_name: "0".to_string(),
    };
    let mut log_mut = log.lock().await;
    log_mut.write(op.clone(), log.clone()).await.unwrap();

    let ops_read_future = log_mut.read(0..2, Duration::from_secs(0), log.clone());
    drop(log_mut);
    let ops_read = ops_read_future.await.unwrap();
    assert_eq!(ops_read, LogResponse::Operations(vec![op]));
}
