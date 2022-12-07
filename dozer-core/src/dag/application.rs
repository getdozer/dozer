use crate::dag::dag::PortDirection::{Input, Output};
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InvalidNodeHandle, InvalidNodeType, InvalidPortHandle};
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};

use std::collections::HashMap;
use std::sync::Arc;
