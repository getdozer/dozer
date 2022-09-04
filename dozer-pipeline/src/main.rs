mod nodes;
mod record;
mod filter_node;
mod executor;
mod dag_executor;
mod dag;

use std::borrow::Borrow;
use std::cell::Ref;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::{mpsc};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinHandle};
use crate::dag::{Edge, InputEdge, InternalEdge, Node, OutputEdge, Processor, Where};
use crate::record::{Field, Record, Schema};


async fn run_dag(nodes: Vec<Node>, edges: Vec<Edge>) {

    let mut nodes_idx: HashMap<u16, Arc<Box<dyn Processor>>> = HashMap::new();
    for node in nodes {
        nodes_idx.insert(node.id, Arc::new(node.processor));
    }

    let mut inputs: HashMap<u8, UnboundedSender<(u8, Record)>> = HashMap::new();
    let mut outputs: HashMap<u8, UnboundedReceiver<(u8, Record)>> = HashMap::new();
    let mut internal_senders : HashMap<u32, UnboundedSender<(u8, Record)>> = HashMap::new();
    let mut internal_receivers : Vec<(u16, u8, UnboundedReceiver<(u8, Record)>)> = Vec::new();

    for edge in edges {

        let (mut tx, mut rx) = mpsc::unbounded_channel::<(u8,Record)>();
        match edge {
            Edge::input(edge) => {
                inputs.insert(edge.input, tx);
                internal_receivers.push((edge.to_node, edge.to_port, rx));
            }
            Edge::output(edge) => {
                outputs.insert(edge.output, rx);
                let node_port : u32 = u32::from(edge.from_node) << 16 | u32::from(edge.from_port);
                internal_senders.insert(node_port, tx);
            }
            Edge::internal(edge) => {
                let from_node_port : u32 = u32::from(edge.from_node) << 16 | u32::from(edge.from_port);
                internal_senders.insert(from_node_port, tx);
                let to_node_port : u32 = u32::from(edge.to_node) << 16 | u32::from(edge.to_port);
                internal_receivers.push((edge.to_node, edge.to_port, rx));
            }
        }
    }

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for t in internal_receivers {

        let mut receiver = t.2;
        let mut cloned_processor = nodes_idx.get(&t.0).unwrap().clone();

        handles.push(tokio::spawn(async move {
            loop {
                println!("starting to listen");
                let result = receiver.recv().await;
                if result.is_none() {
                    return;
                }

            }
        }));
    }

    futures::future::join_all(handles).await;

}



#[tokio::main]
async fn main() {


    let nodes = vec![
        Node::new(100, Box::new(Where::new())),
        Node::new(200, Box::new(Where::new()))
    ];

    let pipes = vec![
        Edge::input(InputEdge::new(1, 100, 1)),
        Edge::internal(InternalEdge::new(100, 1, 200, 1)),
        Edge::output(OutputEdge::new(1, 200,1))
    ];


    run_dag(nodes, pipes).await


    // let dag = DagExecutor::new();
    //
    // let filter_node_0 = FilterNode::new(
    //     NodeConfig::new(vec![1], vec![1]),
    //     FilterNodeConfig::new()
    // );
    //
    // let filter_node_1 = FilterNode::new(
    //     NodeConfig::new(vec![1], vec![1]),
    //     FilterNodeConfig::new()
    // );
    //
    //
    //
    //
    //
    //
    //
    //
    // let (mut tx, mut rx) = mpsc::unbounded_channel::<String>();
    // let ctx = ExecutionContext::new();
    //
    // let node  = FilterNode::new(NodeConfig::new( vec![1], vec![1]), FilterNodeConfig::new());
    //
    // tokio::spawn(async move {node.process(0, Record::new(0, vec![]), &ctx)}).await;
    //
    //


}
