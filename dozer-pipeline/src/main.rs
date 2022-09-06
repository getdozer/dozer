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

use crate::execution::dag::{Edge, InputEdge, InternalEdge, Node, OutputEdge, Processor, ExecutionContext, run_dag};
use crate::execution::mem_context::MemoryExecutionContext;
use crate::execution::record::{Field, Record, Schema, Operation};
use crate::execution::where_processor::{Where};


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

    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();

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
