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
use crate::dag_executor::DagExecutor;
use crate::executor::Executor;
use crate::filter_node::{FilterNode, FilterNodeConfig};
use crate::nodes::{ExecutionContext, NodeConfig, Processor};
use crate::record::{Field, Record, Schema};



#[tokio::main]
async fn main() {


    let dag = DagExecutor::new();

    let filter_node_0 = FilterNode::new(
        NodeConfig::new(vec![1], vec![1]),
        FilterNodeConfig::new()
    );

    let filter_node_1 = FilterNode::new(
        NodeConfig::new(vec![1], vec![1]),
        FilterNodeConfig::new()
    );








    let (mut tx, mut rx) = mpsc::unbounded_channel::<String>();
    let ctx = ExecutionContext::new();

    let node  = FilterNode::new(NodeConfig::new( vec![1], vec![1]), FilterNodeConfig::new());

    tokio::spawn(async move {node.process(0, Record::new(0, vec![]), &ctx)}).await;




}
