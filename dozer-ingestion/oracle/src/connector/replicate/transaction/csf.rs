//! Handles the Continuation SQL flag in V$LOGMNR_CONTENTS.

use crate::connector::replicate::log::LogManagerContent;

/// Output items is guaranteed to have CSF = 0.
pub fn process(
    iterator: impl Iterator<Item = LogManagerContent>,
) -> impl Iterator<Item = LogManagerContent> {
    Processor {
        iterator,
        pending: None,
    }
}

struct Processor<I: Iterator<Item = LogManagerContent>> {
    iterator: I,
    pending: Option<LogManagerContent>,
}

impl<I: Iterator<Item = LogManagerContent>> Iterator for Processor<I> {
    type Item = LogManagerContent;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let content = self.iterator.next()?;

            if let Some(mut previous_content) = self.pending.take() {
                previous_content.sql_redo = match (previous_content.sql_redo, content.sql_redo) {
                    (Some(mut previous), Some(current)) => {
                        previous.push_str(&current);
                        Some(previous)
                    }
                    (previous, current) => previous.or(current),
                };
                if content.csf == 0 {
                    previous_content.csf = 0;
                    return Some(previous_content);
                } else {
                    self.pending = Some(previous_content);
                }
            } else if content.csf == 0 {
                return Some(content);
            } else {
                self.pending = Some(content);
            }
        }
    }
}
