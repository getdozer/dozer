use std::{
    borrow::Cow,
    fmt::{self, Display, Formatter},
    io::{stderr, IsTerminal},
};

use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use metrics::{IntoLabels, Label, SharedString};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Labels(Vec<Label>);

impl IntoLabels for Labels {
    fn into_labels(self) -> Vec<Label> {
        self.0
    }
}

impl Display for Labels {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some((last, labels)) = self.0.split_last() {
            for label in labels {
                write!(f, "{}_{}", label.key(), label.value())?;
                write!(f, "_")?;
            }
            write!(f, "{}_{}", last.key(), last.value())?;
        }
        Ok(())
    }
}

impl Labels {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|label| (label.key(), label.value()))
    }

    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0)
    }

    pub fn push(&mut self, key: impl Into<SharedString>, value: impl Into<SharedString>) {
        self.0.push(Label::new(key, value))
    }

    pub fn to_non_empty_string(&self) -> Cow<'static, str> {
        if self.0.is_empty() {
            Cow::Borrowed("empty")
        } else {
            Cow::Owned(self.to_string())
        }
    }
}

impl<Key: Into<SharedString>, Value: Into<SharedString>> FromIterator<(Key, Value)> for Labels {
    fn from_iter<T: IntoIterator<Item = (Key, Value)>>(iter: T) -> Self {
        let mut labels = Self::new();
        for (key, value) in iter {
            labels.push(key, value);
        }
        labels
    }
}

#[derive(Debug, Clone)]
/// Dozer components make themselves observable through two means:
///
/// - Using `metrics` to emit metrics.
/// - Updating a progress bar.
///
/// Here we define a struct that holds both the metrics labels and the multi progress bar.
/// All components should include labels in this struct and create progress bar from the multi progress bar.
pub struct LabelsAndProgress {
    labels: Labels,
    progress: MultiProgress,
}

impl LabelsAndProgress {
    pub fn new(labels: Labels, enable_progress: bool) -> Self {
        let progress_draw_target = if enable_progress && stderr().is_terminal() {
            ProgressDrawTarget::stderr()
        } else {
            ProgressDrawTarget::hidden()
        };
        let progress = MultiProgress::with_draw_target(progress_draw_target);
        Self { labels, progress }
    }

    pub fn labels(&self) -> &Labels {
        &self.labels
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

impl Default for LabelsAndProgress {
    fn default() -> Self {
        Self::new(Labels::default(), false)
    }
}
