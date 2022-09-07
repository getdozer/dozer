mod execution;

use async_trait::async_trait;
use std::borrow::Borrow;
use std::cell::Ref;
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use futures::executor::block_on;
use futures::future::{join_all, select_all};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use futures::stream::iter;
use tokio::time::sleep;

use crate::execution::dag::{Edge, InputEdge, InternalEdge, Node, OutputEdge, Processor, ExecutionContext, run_dag};
use crate::execution::mem_context::MemoryExecutionContext;
use crate::execution::record::{Field, Operation, Record, Schema};


// async fn sender(tx: UnboundedSender<Operation>, count: u64) {
//
//     let mut ctr = 0;
//     println!("Starting sender");
//     loop {
//         if (ctr > count) {
//             let r = tx.send(Operation::terminate);
//             return;
//         }
//         let r = tx.send(
//             Operation::insert { table_id: ctr, new: Record::new(1, vec![]) }
//         );
//         ctr += 1;
//     }
//
// }
//
// async fn receiver(mut rx: UnboundedReceiver<Operation>, count: u64) {
//
//     println!("Starting receiver");
//     let mut ctr = 0;
//     loop {
//         let r = rx.recv().await;
//         if (r.is_none()) {
//             return;
//         }
//         match r.unwrap() {
//             Operation::insert {table_id, new} => {
//                 assert_eq!(table_id, ctr);
//                 ctr += 1;
//             }
//             Operation::terminate => {
//                 assert_eq!(ctr-1, count);
//                 return;
//             }
//             _ => {
//                 assert!(false)
//             }
//         }
//     }
//
// }

//#[tokio::main]
//async fn main() {

  //   log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
  //
  //   let ctx = Arc::new(MemoryExecutionContext::new());
  //
  //   let (mut input_tx, mut input_rx) = mpsc::unbounded_channel::<Operation>();
  //   let (mut input2_tx, mut input2_rx) = mpsc::unbounded_channel::<Operation>();
  //   let (mut output_tx, mut output_rx) = mpsc::unbounded_channel::<Operation>();
  //
  //   let nodes = vec![
  //       Node::new(100, Box::new(Where::new())),
  //       Node::new(200, Box::new(Where::new())),
  //       Node::new(300, Box::new(Where::new())),
  //       Node::new(400, Box::new(Where::new()))
  //   ];
  //
  //   let edges = vec![
  //       Edge::input(InputEdge::new(1, input_rx, 100, 1)),
  //  //     Edge::input(InputEdge::new(2, input2_rx, 100, 2)),
  //       Edge::internal(InternalEdge::new(100, 1, 200, 1)),
  //       Edge::internal(InternalEdge::new(200, 1, 300, 1)),
  //       Edge::internal(InternalEdge::new(300, 1, 400, 1)),
  //       Edge::output(OutputEdge::new(1, output_tx, 400,1))
  //   ];
  //
  //
  //   let r1 = tokio::spawn(run_dag(nodes, edges, ctx));
  //   let r3 = tokio::spawn(receiver(output_rx));
  //   let r2 = tokio::spawn(sender(input_tx));
  // //  let r4 = tokio::spawn(sender(input2_tx));
  //
  // //  futures::future::join4(r1, r2, r3, r4).await;
  //   futures::future::join3(r1, r2, r3).await;
  //

//}


struct EmptyProcessor {

}

impl EmptyProcessor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Processor for EmptyProcessor {

    async fn init(&mut self, ctx: &dyn ExecutionContext) {

    }

    async fn process(&mut self, data: (u8, Operation), ctx: &dyn ExecutionContext) -> Vec<(u8, Operation)> {

        match data.1 {
            Operation::insert {table_id, new} => {
                return vec![(1, Operation::insert {table_id: table_id, new: Record::new(new.schema_id, vec![])})];
            }
            _ => {
                vec![data]
            }
        }


    }

}

struct EmptyExecutionContext {

}

impl EmptyExecutionContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl ExecutionContext for EmptyExecutionContext {

}


async fn sender(tx: UnboundedSender<Operation>, table_id: u64, count: u64) {

    let mut ctr = 0;
    println!("Starting sender");
    loop {
        if (ctr > count) {
            sleep(Duration::from_secs(5)).await;
            let r = tx.send(Operation::terminate);
            return;
        }
        let r = tx.send(
            Operation::insert { table_id: table_id, new: Record::new(ctr, vec![]) }
        );
        ctr += 1;
    }

}

async fn receiver(mut rx: UnboundedReceiver<Operation>, tables: u64, count: u64) -> (bool, Option<String>) {

    println!("Starting receiver");

    let mut idx: HashMap<u64, u64> = HashMap::new();

    loop {
        let r = rx.recv().await;
        if (r.is_none()) {
            let mut tot : u64 = 0;
            for e in idx.iter() {
                tot += (*e.1 - 1);
            }

            let res = tot == count*tables;
            if res {return (true, None); } else { return (false, Some("Missing records at the end".to_string())); }
        }
        match r.unwrap() {
            Operation::insert {table_id, new} => {

                if idx.contains_key(&table_id) {
                    let prev = idx.get(&table_id).unwrap();
                    if new.schema_id != *prev {
                        return (false, Some("Message count mismatch".to_string()));
                    }
                    idx.insert(table_id.clone(), *prev + 1);
                }
                else {
                    idx.insert(table_id, 1);
                    if new.schema_id != 0 {
                        return (false, Some("Message count mismatch".to_string()));
                    }
                }

                let curr_val = *idx.get(&table_id).unwrap();
            }
            Operation::terminate => {

                // let mut tot : u64 = 0;
                // for e in idx.iter() {
                //     tot += (*e.1 - 1);
                // }
                //
                // let res = tot == count*tables;
                // if res {return (true, None); } else { return (false, Some("Missing records at the end".to_string())); }
            }
            _ => {
                return (false, Some("Unknown message".to_string()));
            }
        }
    }
}


#[tokio::main]
async fn main() {

       // log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
        let ctx = Arc::new(EmptyExecutionContext::new());
        //
        let (mut input_tx, mut input_rx) = mpsc::unbounded_channel::<Operation>();
        let (mut input2_tx, mut input2_rx) = mpsc::unbounded_channel::<Operation>();
        let (mut output_tx, mut output_rx) = mpsc::unbounded_channel::<Operation>();

        let nodes = vec![
            Node::new(100, Box::new(EmptyProcessor::new())),
            Node::new(200, Box::new(EmptyProcessor::new())),
            Node::new(300, Box::new(EmptyProcessor::new())),
            Node::new(400, Box::new(EmptyProcessor::new()))
        ];

        let edges = vec![
            Edge::input(InputEdge::new(1, input_rx, 100, 1)),
            //     Edge::input(InputEdge::new(2, input2_rx, 100, 2)),
            Edge::internal(InternalEdge::new(100, 1, 200, 1)),
            Edge::internal(InternalEdge::new(200, 1, 300, 1)),
            Edge::internal(InternalEdge::new(300, 1, 400, 1)),
            Edge::output(OutputEdge::new(1, output_tx, 400, 1))
        ];


        let r1 = tokio::spawn(run_dag(nodes, edges, ctx));
        let r3 = tokio::spawn(receiver(output_rx, 1, 200000));
        let r2 = tokio::spawn(sender(input_tx, 1, 200000));
        //  let r4 = tokio::spawn(sender(input2_tx));

        //  futures::future::join4(r1, r2, r3, r4).await;
        let exec = futures::future::join3(r1, r2, r3).await;

}

#[tokio::test]
async fn test_pipeline() {

   // log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    let ctx = Arc::new(EmptyExecutionContext::new());
    //
    let (mut input_tx, mut input_rx) = mpsc::unbounded_channel::<Operation>();
    let (mut input2_tx, mut input2_rx) = mpsc::unbounded_channel::<Operation>();
    let (mut output_tx, mut output_rx) = mpsc::unbounded_channel::<Operation>();

    let nodes = vec![
        Node::new(100, Box::new(EmptyProcessor::new())),
        Node::new(200, Box::new(EmptyProcessor::new())),
        Node::new(300, Box::new(EmptyProcessor::new())),
        Node::new(400, Box::new(EmptyProcessor::new()))
    ];

    let edges = vec![
        Edge::input(InputEdge::new(1, input_rx, 100, 1)),
        Edge::input(InputEdge::new(2, input2_rx, 100, 2)),
        Edge::internal(InternalEdge::new(100, 1, 200, 1)),
        Edge::internal(InternalEdge::new(200, 1, 300, 1)),
        Edge::internal(InternalEdge::new(300, 1, 400, 1)),
        Edge::output(OutputEdge::new(1, output_tx, 400, 1))
    ];


    let dag_thread = tokio::spawn(run_dag(nodes, edges, ctx));
    let receiver_thread = tokio::spawn(receiver(output_rx, 2,1000000));
    let sender_thread_1 = tokio::spawn(sender(input_tx, 1, 1000000));
    let sender_thread_2 = tokio::spawn(sender(input2_tx, 2, 1000000));

    let execResult = futures::future::join4(
        dag_thread, receiver_thread,
        sender_thread_1,
        sender_thread_2
    ).await;

    assert!(execResult.1.unwrap().0);




}


