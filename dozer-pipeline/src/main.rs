mod nodes;
mod record;
mod filter_node;
mod executor;
mod dag_executor;

use std::borrow::Borrow;
use std::cell::Ref;
use std::collections::HashMap;
use std::future::Future;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::mpsc;
use crate::filter_node::{FilterNode, FilterNodeConfig};
use crate::nodes::{ExecutionContext, NodeConfig, Processor};
use crate::record::{Field, Record, Schema};



#[tokio::main]
async fn main() {



    let (mut tx, mut rx) = mpsc::channel::<String>(1000);
    let ctx = ExecutionContext::new();

    let node  = FilterNode::new(NodeConfig::new( vec![1], vec![1]), FilterNodeConfig::new());

    tokio::spawn(async move {node.process(0, Record::new(0, vec![]), &ctx)}).await;




}
