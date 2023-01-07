use rustyline::highlight::Highlighter;
use rustyline::hint::{Hint, Hinter};
use rustyline::history::SearchDirection;
use rustyline::{
    Cmd, ConditionalEventHandler, Context, Event, EventContext, KeyEvent, RepeatCount,
};
use rustyline_derive::{Completer, Helper, Validator};
use std::borrow::Cow::{self, Borrowed, Owned};
use std::collections::{HashMap, HashSet};

#[derive(Completer, Helper, Validator)]
pub struct ConfigureHelper {
    // TODO: replace with radix tree
    pub hints: HashSet<CommandHint>,
}

#[derive(Hash, Debug, PartialEq, Eq)]
pub struct CommandHint {
    display: String,
    complete_up_to: usize,
}

impl Hint for CommandHint {
    fn display(&self) -> &str {
        &self.display
    }

    fn completion(&self) -> Option<&str> {
        if self.complete_up_to > 0 {
            Some(&self.display[..self.complete_up_to])
        } else {
            None
        }
    }
}

impl CommandHint {
    fn new(text: &str, complete_up_to: &str) -> CommandHint {
        assert!(text.starts_with(complete_up_to));
        CommandHint {
            display: text.into(),
            complete_up_to: complete_up_to.len(),
        }
    }

    fn suffix(&self, strip_chars: usize) -> CommandHint {
        CommandHint {
            display: self.display[strip_chars..].to_owned(),
            complete_up_to: self.complete_up_to.saturating_sub(strip_chars),
        }
    }
}

impl Hinter for ConfigureHelper {
    type Hint = CommandHint;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<CommandHint> {
        if line.is_empty() || pos < line.len() {
            return None;
        }

        let hint = self
            .hints
            .iter()
            .filter_map(|hint| {
                // expect hint after word complete, like redis cli, add condition:
                // line.ends_with(" ")
                if hint.display.starts_with(line) {
                    Some(hint.suffix(pos))
                } else {
                    None
                }
            })
            .next();

        if let Some(hint) = hint {
            Some(hint)
        } else {
            let start = if ctx.history_index() == ctx.history().len() {
                ctx.history_index().saturating_sub(1)
            } else {
                ctx.history_index()
            };
            if let Some(sr) = ctx
                .history()
                .starts_with(line, start, SearchDirection::Reverse)
            {
                if sr.entry == line {
                    return None;
                }
                let str = sr.entry[pos..].to_owned();
                let len = str.len();
                return Some(CommandHint {
                    display: str,
                    complete_up_to: len,
                });
            }
            None
        }
    }
}
impl Highlighter for ConfigureHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Owned(format!("\x1b[1;32m{prompt}\x1b[m"))
        } else {
            Borrowed(prompt)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned(format!("\x1b[1m{hint}\x1b[m"))
    }
}

pub fn hints() -> HashSet<CommandHint> {
    let mut set = HashSet::new();
    set.insert(CommandHint::new("help", "help"));
    set.insert(CommandHint::new("show sources", "show sources"));
    set.insert(CommandHint::new("test connections", "test connections"));
    set.insert(CommandHint::new("select col1 from table_name", "select "));
    set
}

#[derive(Debug, Clone)]
pub enum DozerCmd {
    Help,
    ShowSources,
    Sql(String),
    Exit,
}
pub fn get_commands() -> HashMap<String, DozerCmd> {
    let mut map = HashMap::new();
    map.insert("help".to_string(), DozerCmd::Help);
    map.insert("show sources".to_string(), DozerCmd::ShowSources);
    map.insert("test connections".to_string(), DozerCmd::ShowSources);
    map.insert("exit".to_string(), DozerCmd::Exit);

    map
}

pub struct TabEventHandler;
impl ConditionalEventHandler for TabEventHandler {
    fn handle(&self, evt: &Event, _: RepeatCount, _: bool, ctx: &EventContext) -> Option<Cmd> {
        debug_assert_eq!(*evt, Event::from(KeyEvent::from('\t')));
        if !ctx.has_hint() {
            return None; // default
        }
        evt.get(0).map(|_k| Cmd::CompleteHint)
    }
}
