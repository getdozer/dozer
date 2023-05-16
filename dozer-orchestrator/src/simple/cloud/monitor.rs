use crate::errors::DeployError;
use dozer_types::grpc_types::cloud::dozer_cloud_client::DozerCloudClient;
use dozer_types::grpc_types::cloud::StatusUpdate;
use dozer_types::grpc_types::cloud::StatusUpdateRequest;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub fn monitor_app(
    app_id: String,
    target_url: String,
    runtime: Arc<Runtime>,
) -> Result<(), DeployError> {
    runtime.block_on(async move {
        let mut client: DozerCloudClient<tonic::transport::Channel> =
            DozerCloudClient::connect(target_url).await?;
        let mut response = client
            .on_status_update(StatusUpdateRequest { app_id })
            .await?
            .into_inner();

        let mut bars: HashMap<String, ProgressBar> = HashMap::new();
        let m = MultiProgress::new();

        while let Some(StatusUpdate { source, count, .. }) = response.message().await? {
            match bars.get(&source) {
                None => {
                    let pb = attach_progress(&m);
                    pb.set_message(source.clone());
                    pb.set_position(count);
                    bars.insert(source.clone(), pb);
                }
                Some(bar) => {
                    bar.set_position(count);
                }
            };
        }

        Ok::<(), DeployError>(())
    })?;

    Ok(())
}

fn attach_progress(m: &MultiProgress) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    m.add(pb.clone());
    pb.set_style(
        ProgressStyle::with_template("{spinner:.red} {msg}: {pos}: {per_sec}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}
