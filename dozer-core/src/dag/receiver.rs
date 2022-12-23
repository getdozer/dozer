use crate::dag::executor::ExecutorOperation;
use crossbeam::channel::{Receiver, RecvError, Select};
use std::collections::HashSet;

struct SelectReceiver<'a> {
    receivers: Vec<Receiver<ExecutorOperation>>,
    select: Select<'a>,
}

impl<'a> SelectReceiver<'a> {}
