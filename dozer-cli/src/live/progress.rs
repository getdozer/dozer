use std::{collections::HashMap, ops::Deref, sync::atomic::Ordering, time::Duration};

use dozer_types::grpc_types::live::{ConnectResponse, Metric, ProgressResponse};
use prometheus_parse::Value;
use tokio::time::interval;

use crate::shutdown::ShutdownReceiver;

use super::LiveError;

const PROGRESS_POLL_FREQUENCY: u64 = 100;
const METRICS_ENDPOINT: &str = "http://localhost:9000/metrics";
pub async fn progress_stream(
    tx: tokio::sync::broadcast::Sender<ConnectResponse>,
    shutdown_receiver: ShutdownReceiver,
) -> Result<(), LiveError> {
    let mut retry_interval = interval(Duration::from_millis(PROGRESS_POLL_FREQUENCY));

    let mut progress: HashMap<String, Metric> = HashMap::new();

    loop {
        if !shutdown_receiver.get_running_flag().load(Ordering::Relaxed) {
            return Ok(());
        }
        let lines = match reqwest::get(METRICS_ENDPOINT).await {
            Ok(texts) => texts.text().await.map_or(vec![], |body| {
                body.lines().map(|s| Ok(s.to_owned())).collect()
            }),
            Err(e) => {
                return Err(LiveError::BoxedError(Box::new(e)));
            }
        };

        if let Ok(metrics) = prometheus_parse::Scrape::parse(lines.into_iter()) {
            for sample in metrics.samples {
                if let Value::Counter(count) = sample.value {
                    progress.insert(
                        sample.metric,
                        Metric {
                            value: count as u32,
                            labels: sample.labels.deref().clone(),
                            ts: sample.timestamp.timestamp_millis() as u32,
                        },
                    );
                }
            }

            tx.send(ConnectResponse {
                live: None,
                progress: Some(ProgressResponse {
                    progress: progress.clone(),
                }),
            })
            .map_err(|e| LiveError::SendError(e))?;
            retry_interval.tick().await;
        } else {
            retry_interval.tick().await;
        }
    }
}
