mod execution;

use std::borrow::Borrow;
use std::cell::Ref;
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use futures::future::{join_all, select_all};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use futures::stream::iter;

use crate::execution::dag::{Edge, InputEdge, InternalEdge, Node, OutputEdge, Processor, ExecutionContext};
use crate::execution::mem_context::MemoryExecutionContext;
use crate::execution::record::{Field, Record, Schema, Operation};
use crate::execution::where_processor::{Where};


async fn run_dag(nodes: Vec<Node>, edges: Vec<Edge>, ctx: Arc<dyn ExecutionContext>) {

    let mut senders : HashMap<u16, HashMap<u8, UnboundedSender<Operation>>> = HashMap::new();
    let mut receivers : HashMap<u16, Vec<(u8, UnboundedReceiver<Operation>)>>  = HashMap::new();

    for node in &nodes {
        senders.insert(node.id, HashMap::new());
        receivers.insert(node.id, Vec::new());
    }

    for mut edge in edges {
        match edge {
            Edge::input(edge) => {
                receivers.get_mut(&edge.to_node).unwrap().push((edge.to_port, edge.input));
            }
            Edge::output(edge) => {
                senders.get_mut(&edge.from_node).unwrap().insert(edge.from_port, edge.output);
            }
            Edge::internal(edge) => {
                let (mut tx, mut rx) = mpsc::unbounded_channel::<Operation>();
                receivers.get_mut(&edge.to_node).unwrap().push((edge.to_port, rx)) ;
                senders.get_mut(&edge.from_node).unwrap().insert(edge.from_port, tx);
            }
        }
    }

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for mut node in nodes {

        let mut node_receivers = receivers.remove(&node.id).unwrap();
        let mut node_senders = senders.remove(&node.id).unwrap();

        if node_receivers.len() == 1 {

            let mut receiver = node_receivers.remove(0);
            let cloned_ctx = ctx.clone();

            let handle = tokio::spawn(async move {

                let mut senders = node_senders;
                loop {
                    let res = receiver.1.recv().await;
                    if res.is_none() {
                        println!("Exiting read loop for node/port {}/{}", node.id, receiver.0);
                        return;
                    }
                    println!("Incoming record on node {} / port {}", node.id, receiver.0);
                    let processed = node.processor.process((receiver.0, res.unwrap()), cloned_ctx.as_ref()).await;
                    for rec in processed {
                        let sender = senders.get_mut(&rec.0);
                        if (!sender.is_none()) {
                            println!("Forwarding message from node {} / port {}", node.id, rec.0);
                            sender.unwrap().send(rec.1);
                        }
                    }
                }
            });
        }
        else {

            let mut m_node_senders : HashMap<u8, Arc<Mutex<UnboundedSender<Operation>>>> = HashMap::new();
            for mut t in node_senders {
                m_node_senders.insert(t.0, Arc::new(Mutex::new(t.1)));
            }

            let node_id = node.id;
            let mut m_node_processor = Arc::new(Mutex::new(node.processor));

            for mut receiver in node_receivers {

                let mut m_node_processor_clone = m_node_processor.clone();
                let mut m_node_senders_clone = m_node_senders.clone();
                let cloned_ctx = ctx.clone();

                let handle = tokio::spawn(async move {
                    loop {
                        let res = receiver.1.recv().await;
                        if res.is_none() {
                            println!("Exiting read loop for node/port {}/{}", node_id, receiver.0);
                            return;
                        }
                        println!("Incoming record on node {} / port {}", node_id, receiver.0);
                        let processed = m_node_processor_clone.lock().await.process((receiver.0, res.unwrap()), cloned_ctx.as_ref()).await;
                        for rec in processed {
                            let sender = m_node_senders_clone.get_mut(&rec.0);
                            if (!sender.is_none()) {
                                println!("Forwarding message from node {} / port {}", node.id, rec.0);
                                sender.unwrap().lock().await.send(rec.1);
                            }
                        }
                    }
                });
            }


        }

    }

    futures::future::join_all(handles).await;

}

async fn sender(tx: UnboundedSender<Operation>) {

    let mut ctr = 0;
    println!("Starting sender");
    loop {
        ctr += 1;
       // tokio::time::sleep(Duration::from_millis(1000)).await;
        println!("record {}", &ctr);
        let r = tx.send(Operation::insert {table: 1, record: Record::new(1, vec![])});
        if r.is_err() {
            println!("Error sending");
        }
    }

}

async fn receiver(mut rx: UnboundedReceiver<Operation>) {

    println!("Starting receiver");
    loop {
        let r = rx.recv().await;
        if (r.is_none()) {
            return;
        }
        println!("Received");
    }

}

#[tokio::main]
async fn main() {

    let ctx = Arc::new(MemoryExecutionContext::new());

    let (mut input_tx, mut input_rx) = mpsc::unbounded_channel::<Operation>();
    let (mut input2_tx, mut input2_rx) = mpsc::unbounded_channel::<Operation>();
    let (mut output_tx, mut output_rx) = mpsc::unbounded_channel::<Operation>();

    let nodes = vec![
        Node::new(100, Box::new(Where::new())),
        Node::new(200, Box::new(Where::new())),
        Node::new(300, Box::new(Where::new())),
        Node::new(400, Box::new(Where::new()))
    ];

    let edges = vec![
        Edge::input(InputEdge::new(1, input_rx, 100, 1)),
        Edge::input(InputEdge::new(2, input2_rx, 100, 2)),
        Edge::internal(InternalEdge::new(100, 1, 200, 1)),
        Edge::internal(InternalEdge::new(200, 1, 300, 1)),
        Edge::internal(InternalEdge::new(300, 1, 400, 1)),
        Edge::output(OutputEdge::new(1, output_tx, 400,1))
    ];


    let r1 = tokio::spawn(run_dag(nodes, edges, ctx));
    let r3 = tokio::spawn(receiver(output_rx));
    let r2 = tokio::spawn(sender(input_tx));
    let r4 = tokio::spawn(sender(input2_tx));

    futures::future::join4(r1, r2, r3, r4).await;
  //  futures::future::join3(r1, r2, r3).await;





}
