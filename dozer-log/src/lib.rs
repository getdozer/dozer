pub mod errors;
pub mod home_dir;
pub mod reader;
pub mod replication;
pub mod schemas;
pub use camino;
pub use dyn_clone;
pub mod storage;

use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
pub use tokio;

pub fn attach_progress(multi_pb: MultiProgress) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    multi_pb.add(pb.clone());
    pb.set_style(
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
    pb
}
