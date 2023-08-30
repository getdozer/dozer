use std::{
    borrow::Cow,
    fmt::{self, Display, Formatter},
};

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
