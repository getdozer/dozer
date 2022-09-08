mod execution;

use async_trait::async_trait;
use std::borrow::Borrow;
use std::cell::Ref;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::execution::dag::{Edge, InputEdge, InternalEdge, Node, OutputEdge, Processor, ExecutionContext, run_dag};
use crate::execution::mem_context::MemoryExecutionContext;
use crate::execution::record::{Field, Operation, Record, Schema};
use crate::execution::pipeline_builder::PipelineBuilder;


struct EmptyProcessor {

}

impl EmptyProcessor {
    pub fn new() -> Self {
        Self {}
    }
}

impl Processor for EmptyProcessor {

    fn init(&mut self, ctx: &dyn ExecutionContext) {

    }

    fn process(&mut self, data: (u8, Operation), ctx: &dyn ExecutionContext) -> Vec<(u8, Operation)> {

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


fn sender(tx: Arc<Mutex<Sender<Operation>>>, table_id: u64, count: u64) -> u64 {

    let writer = tx.lock().unwrap();
    let mut ctr = 0;
    println!("Starting sender");
    loop {
        if (ctr >= count) {
            return ctr;
        }
        let r = writer.send(
            Operation::insert { table_id: table_id, new: Record::new(ctr, vec![]) }
        );
        ctr += 1;
    }

}

fn receiver(mut rx: Receiver<Operation>, tables: u64, count: u64) -> (bool, Option<String>) {

    println!("Starting receiver");

    let mut idx: HashMap<u64, u64> = HashMap::new();

    loop {
        let r = rx.recv();
        if (!r.is_ok()) {
            return (false, Some("Error reading".to_string()));
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


                let mut tot : u64 = 0;
                for e in idx.iter() {
                    tot += *e.1;
                }
                let res = tot == count*tables;
                if res {return (true, None); }

            }

            _ => {
                return (false, Some("Unknown message".to_string()));
            }
        }
    }
}

fn main() {


}

#[test]
fn test_pipeline_builder() {
let sql = "SELECT Country, COUNT(CustomerID), SUM(Spending) \
                        FROM Customers \
                        WHERE Spending >= 1000 \
                        GROUP BY Country \
                        HAVING COUNT(CustomerID) > 1;";

let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

let ast = Parser::parse_sql(&dialect, sql).unwrap();

println!("AST: {:?}", ast);

let statement = &ast[0];

let (nodes, edges) = PipelineBuilder::statement_to_pipeline(statement.clone()).unwrap();
}

#[test]
fn test_pipeline() {

  //   log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
    let ctx = Arc::new(EmptyExecutionContext::new());
    //
    let (mut input_tx, mut input_rx) = channel::<Operation>();
    let (mut input2_tx, mut input2_rx) = channel::<Operation>();
    let (mut output_tx, mut output_rx) = channel::<Operation>();

    let nodes = vec![
        Node::new(100, Box::new(EmptyProcessor::new())),
        Node::new(200, Box::new(EmptyProcessor::new())),
        Node::new(300, Box::new(EmptyProcessor::new())),
        Node::new(400, Box::new(EmptyProcessor::new())),
        Node::new(500, Box::new(EmptyProcessor::new())),
        Node::new(600, Box::new(EmptyProcessor::new())),
        Node::new(700, Box::new(EmptyProcessor::new()))
    ];

    let edges = vec![
        Edge::input(InputEdge::new(1, input_rx, 100, 1)),
     //   Edge::input(InputEdge::new(2, input2_rx, 100, 2)),
        Edge::internal(InternalEdge::new(100, 1, 200, 1)),
        Edge::internal(InternalEdge::new(200, 1, 300, 1)),
        Edge::internal(InternalEdge::new(300, 1, 400, 1)),
        Edge::internal(InternalEdge::new(400, 1, 500, 1)),
        Edge::internal(InternalEdge::new(500, 1, 600, 1)),
        Edge::internal(InternalEdge::new(600, 1, 700, 1)),
        Edge::output(OutputEdge::new(1, output_tx, 700, 1))
    ];

    let input_tx_arc = Arc::new(Mutex::new(input_tx.clone()));
    let input_tx2_arc = Arc::new(Mutex::new(input2_tx.clone()));

    let sender_thread_1 = thread::spawn(move || {sender(input_tx_arc, 1, 10000000)});
  //  let sender_thread_2 = thread::spawn(move || {sender(input_tx2_arc, 2, 5000000)});

    let dag_thread = thread::spawn(|| {run_dag(nodes, edges, ctx);});
    let receiver_thread = thread::spawn(|| {receiver(output_rx, 1, 10000000)});

    let r = sender_thread_1.join().unwrap();
  //  let r2 = sender_thread_2.join().unwrap();
    let r3 = receiver_thread.join().unwrap();


  //  input_tx.send(Operation::terminate);

   // dag_thread.join().unwrap();

}

#[test]
fn test2() {

    let ec = EmptyExecutionContext::new();
    let mut p = Box::new(EmptyProcessor::new());

    for n in 1..10000000 {

        for n1 in 0..10 {
            let mut data = Operation::insert { table_id: 1, new: Record::new(1, vec![]) };
            let r = p.process((1, data), &ec);
        }
    }




}



use blockingqueue::BlockingQueue;



#[test]
fn test() {
    let bq = BlockingQueue::new();

    let bq_clone1 = bq.clone();
    thread::spawn(move || {
        let mut c = 1;
        loop {
            if c > 1000000 {
                return
            }
            bq_clone1.push((1, Record::new(1, vec![Field::int_field(10)])));
            c += 1;
        }
    });

    let bq_clone2 = bq.clone();
    thread::spawn(move || {
        let mut c = 1;
        loop {
            if c > 1000000 {
                return
            }
            bq_clone2.push((1, Record::new(1, vec![Field::int_field(10)])));
            c += 1;
        }
    });

    let bq_clone3 = bq.clone();
    let reader = thread::spawn(move || {
        let mut c = 0;
        loop {
            if c >= 2000000 {
                return
            }
            let r  = bq_clone3.pop();
            println!("{}", r.0);
            c += 1;
        }
    });

    reader.join().unwrap();

    println!("I will wait forever here...");
    // println!("{}", bq.pop());
}



