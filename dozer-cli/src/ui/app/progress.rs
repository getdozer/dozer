use std::{collections::HashMap, ops::Deref, sync::atomic::Ordering, time::Duration};

use dozer_api::shutdown::ShutdownReceiver;
use dozer_types::grpc_types::app_ui::{ConnectResponse, Metric, ProgressResponse};
use prometheus_parse::Value;
use tokio::time::interval;

use super::AppUIError;

const PROGRESS_POLL_FREQUENCY: u64 = 100;
const METRICS_ENDPOINT: &str = "http://localhost:9000/metrics";
pub async fn progress_stream(
    tx: tokio::sync::broadcast::Sender<ConnectResponse>,
    shutdown_receiver: ShutdownReceiver,
    labels: dozer_tracing::Labels,
) -> Result<(), AppUIError> {
    let mut retry_interval = interval(Duration::from_millis(PROGRESS_POLL_FREQUENCY));

    let mut progress: HashMap<String, Metric> = HashMap::new();

    loop {
        if !shutdown_receiver.get_running_flag().load(Ordering::Relaxed) {
            return Ok(());
        }
        let text = reqwest::get(METRICS_ENDPOINT)
            .await?
            .error_for_status()?
            .text()
            .await?;
        let lines = text.lines().map(|line| Ok(line.to_string()));

        if let Ok(metrics) = prometheus_parse::Scrape::parse(lines) {
            for sample in metrics.samples {
                if let Value::Counter(count) = sample.value {
                    if labels_match(&sample.labels, &labels) {
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
            }

            if tx
                .send(ConnectResponse {
                    app_ui: None,
                    progress: Some(ProgressResponse {
                        progress: progress.clone(),
                    }),
                    build: None,
                })
                .is_err()
            {
                // If the receiver is dropped, we're done here.
                return Ok(());
            }
        }

        retry_interval.tick().await;
    }
}

fn labels_match(
    prom_labels: &prometheus_parse::Labels,
    dozer_labels: &dozer_tracing::Labels,
) -> bool {
    dozer_labels
        .iter()
        .all(|(key, value)| prom_labels.get(key) == Some(value))
}
