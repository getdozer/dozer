use std::borrow::Cow;

use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use opentelemetry::KeyValue;

#[derive(Debug, Clone, Default)]
/// Dozer components make themselves observable through two means:
///
/// - Using `metrics` to emit metrics.
/// - Updating a progress bar.
///
/// Here we define a struct that holds both the metrics labels and the multi progress bar.
/// All components should include labels in this struct and create progress bar from the multi progress bar.
pub struct LabelsAndProgress {
    application_id: String,
    progress: MultiProgress,
}

impl LabelsAndProgress {
    pub fn new(application_id: String, enable_progress: bool) -> Self {
        let progress_draw_target = if enable_progress && atty::is(atty::Stream::Stderr) {
            ProgressDrawTarget::stderr()
        } else {
            ProgressDrawTarget::hidden()
        };
        let progress = MultiProgress::with_draw_target(progress_draw_target);
        Self {
            application_id,
            progress,
        }
    }

    pub fn attrs(&self) -> Vec<KeyValue> {
        vec![KeyValue::new("application_id", self.application_id.clone())]
    }

    pub fn create_progress_bar(&self, msg: impl Into<Cow<'static, str>>) -> ProgressBar {
        let progress = ProgressBar::new_spinner();
        self.progress.add(progress.clone());
        progress.set_style(
            ProgressStyle::with_template("{spinner:.blue} {msg}: {pos}: {per_sec}")
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
        progress.set_message(msg);
        progress
    }
}
