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
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use futures::task::SpawnExt;
use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinHandle};
use crate::dag::{Edge, InputEdge, InternalEdge, Node, OutputEdge, Processor, Where};
use crate::record::{Field, Record, Schema};


async fn run_dag(nodes: Vec<Node>, edges: Vec<Edge>) {

    // let mut nodes_idx: HashMap<u16, Arc<Mutex<dyn Processor>>> = HashMap::new();
    // for node in nodes {
    //     nodes_idx.insert(node.id, Arc::new(Mutex::new(node.processor)));
    // }

    let mut internal_senders : HashMap<u32, UnboundedSender<(u8, Record)>> = HashMap::new();
    let mut internal_receivers : Vec<(u16, u8, UnboundedReceiver<(u8, Record)>)> = Vec::new();

    for edge in edges {

        match edge {
            Edge::input(edge) => {
                internal_receivers.push((edge.to_node, edge.to_port, edge.input));
            }
            Edge::output(edge) => {
                let node_port : u32 = u32::from(edge.from_node) << 16 | u32::from(edge.from_port);
                internal_senders.insert(node_port, edge.output);
            }
            Edge::internal(edge) => {
                let (mut tx, mut rx) = mpsc::unbounded_channel::<(u8,Record)>();
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
        let mut cloned_processor = nodes.first().unwrap().processor.clone();

        handles.push(tokio::spawn(async move {
            loop {
                println!("starting to listen");
                let result = receiver.recv().await;
                if result.is_none() {
                    return;
                }
                cloned_processor.lock().await.process(result.unwrap());
                //cloned_processor.get_mut().process(result.unwrap());
                println!("something received");
            }
        }));
    }

    futures::future::join_all(handles).await;

}

async fn sender(tx: UnboundedSender<(u8, Record)>) {

    while true {
        sleep(Duration::from_secs(1));
        let r = tx.send((1, Record::new(1, vec![])));
        if r.is_err() {
            println!("Error sending");
        }
    }


}

#[tokio::main]
async fn main() {

    let (mut input_tx, mut input_rx) = mpsc::unbounded_channel::<(u8,Record)>();
    let (mut output_tx, mut output_rx) = mpsc::unbounded_channel::<(u8,Record)>();

    let nodes = vec![
        Node::new(100, Arc::new(Mutex::new(Where::new()))),
        Node::new(200, Arc::new(Mutex::new(Where::new()))),
        Node::new(300, Arc::new(Mutex::new(Where::new())))
    ];

    let edges = vec![
        Edge::input(InputEdge::new(1, input_rx, 100, 1)),
        Edge::internal(InternalEdge::new(100, 1, 200, 1)),
        Edge::internal(InternalEdge::new(200, 1, 300, 1)),
        Edge::output(OutputEdge::new(1, output_tx, 300,1))
    ];


    let r1 = run_dag(nodes, edges);
    let r2 = sender(input_tx);
    futures::future::join(r1, r2).await;





}
