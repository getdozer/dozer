use std::sync::Arc;

use dozer_types::{epoch::ExecutorOperation, models::app_config::LogStorage};
use tempdir::TempDir;
use tokio::sync::Mutex;

use crate::replication::{Log, LogResponse};

#[tokio::test]
async fn write_read_mutable() {
    let temp_dir = TempDir::new("write_read_mutable").unwrap();
    let log = Log::new(
        LogStorage::Local(()),
        temp_dir.path().to_str().unwrap().to_string(),
        false,
        10,
    )
    .await
    .unwrap();
    let log = Arc::new(Mutex::new(log));

    let ops = vec![
        ExecutorOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];

    let mut log_mut = log.lock().await;
    for op in &ops {
        log_mut.write(op.clone(), log.clone());
    }
    drop(log_mut);

    let range = 1..ops.len();
    let ops_read = log.lock().await.read(range.clone()).await.unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[tokio::test]
async fn watch_write_mutable() {
    let temp_dir = TempDir::new("watch_write_mutable").unwrap();
    let mut log = Log::new(
        LogStorage::Local(()),
        temp_dir.path().to_str().unwrap().to_string(),
        false,
        10,
    )
    .await
    .unwrap();

    let range = 1..3;
    let handle = tokio::spawn(log.read(range.clone()));

    let log = Arc::new(Mutex::new(log));
    let ops = vec![
        ExecutorOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];
    let mut log_mut = log.lock().await;
    for op in &ops {
        log_mut.write(op.clone(), log.clone());
    }

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[tokio::test]
async fn watch_partial_write_mutable() {
    let temp_dir = TempDir::new("watch_partial_write_mutable").unwrap();
    let mut log = Log::new(
        LogStorage::Local(()),
        temp_dir.path().to_str().unwrap().to_string(),
        false,
        2,
    )
    .await
    .unwrap();

    let handle = tokio::spawn(log.read(1..3));

    let log = Arc::new(Mutex::new(log));
    let ops = vec![
        ExecutorOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];
    let mut log_mut = log.lock().await;
    for op in &ops {
        log_mut.write(op.clone(), log.clone());
    }

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[1..2].to_vec()));
}

#[tokio::test]
async fn watch_out_of_range_write_mutable() {
    let temp_dir = TempDir::new("watch_out_of_range_write_mutable").unwrap();
    let mut log = Log::new(
        LogStorage::Local(()),
        temp_dir.path().to_str().unwrap().to_string(),
        false,
        2,
    )
    .await
    .unwrap();

    let range = 2..3;
    let handle = tokio::spawn(log.read(range.clone()));

    let log = Arc::new(Mutex::new(log));
    let ops = vec![
        ExecutorOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];
    let mut log_mut = log.lock().await;
    for op in &ops {
        log_mut.write(op.clone(), log.clone());
    }

    let ops_read = handle.await.unwrap().unwrap();
    assert_eq!(ops_read, LogResponse::Operations(ops[range].to_vec()));
}

#[tokio::test]
async fn immutable_gets_evict() {
    let temp_dir = TempDir::new("immutable_gets_evict").unwrap();
    let log = Log::new(
        LogStorage::Local(()),
        temp_dir.path().to_str().unwrap().to_string(),
        true,
        2,
    )
    .await
    .unwrap();
    let log = Arc::new(Mutex::new(log));

    let ops = vec![
        ExecutorOperation::SnapshottingDone {
            connection_name: "0".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "1".to_string(),
        },
        ExecutorOperation::SnapshottingDone {
            connection_name: "2".to_string(),
        },
    ];
    let mut log_mut = log.lock().await;
    assert!(log_mut.write(ops[0].clone(), log.clone()).is_none());
    let handle = log_mut.write(ops[1].clone(), log.clone()).unwrap();
    assert!(log_mut.write(ops[2].clone(), log.clone()).is_none());
    drop(log_mut);
    handle.await.unwrap();
    assert!(matches!(
        log.lock().await.read(0..1).await.unwrap(),
        LogResponse::Persisted(_)
    ));
}
