use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::thread::JoinHandle;
use crate::{Operation, Record};
use log::{error, info, warn};
use log4rs;

pub struct InternalEdge {
    pub from_node: u16,
    pub from_port: u8,
    pub to_node: u16,
    pub to_port: u8
}

impl InternalEdge {
    pub fn new(from_node: u16, from_port: u8, to_node: u16, to_port: u8) -> Self {
        Self { from_node, from_port, to_node, to_port }
    }
}

pub struct InputEdge {
    pub input_id: u8,
    pub input: Receiver<Operation>,
    pub to_node: u16,
    pub to_port: u8
}

impl InputEdge {
    pub fn new(input_id: u8, input: Receiver<Operation>, to_node: u16, to_port: u8) -> Self {
        Self { input_id, input, to_node, to_port }
    }
}

pub struct OutputEdge {
    pub output_id: u8,
    pub output: Sender<Operation>,
    pub from_node: u16,
    pub from_port: u8
}

impl OutputEdge {
    pub fn new(output_id: u8, output: Sender<Operation>, from_node: u16, from_port: u8) -> Self {
        Self { output_id, output, from_node, from_port }
    }
}


pub enum Edge {
    internal(InternalEdge),
    input(InputEdge),
    output(OutputEdge)
}


pub struct Node {
    pub id: u16,
    pub processor: Box<dyn Processor>
}

impl Node {
    pub fn new(id: u16, processor: Box<dyn Processor>) -> Node {
        Node{id, processor}
    }

}

pub trait Processor : Send {
    fn init(&mut self, ctx: &dyn ExecutionContext);
    fn process(&mut self, data: (u8, Operation), ctx: &dyn ExecutionContext) -> Vec<(u8, Operation)>;
}


pub trait ExecutionContext : Send + Sync {

}


pub fn run_dag(nodes: Vec<Node>, edges: Vec<Edge>, ctx: Arc<dyn ExecutionContext>) {

    let mut senders : HashMap<u16, HashMap<u8, Sender<Operation>>> = HashMap::new();
    let mut receivers : HashMap<u16, Vec<(u8, Receiver<Operation>)>>  = HashMap::new();

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
                let (mut tx, mut rx) = channel();
                receivers.get_mut(&edge.to_node).unwrap().push((edge.to_port, rx)) ;
                senders.get_mut(&edge.from_node).unwrap().insert(edge.from_port, tx);
            }
        }
    }

  //  let mut handles: Vec<JoinHandle<()>> = Vec::new();

    for mut node in nodes {

        let mut node_receivers = receivers.remove(&node.id).unwrap();
        let mut node_senders = senders.remove(&node.id).unwrap();

        if node_receivers.len() == 1 {

            let mut receiver = node_receivers.remove(0);
            let cloned_ctx = ctx.clone();

            let handle = thread::spawn( move || {

                let mut senders = node_senders;
                loop {
                    let res = receiver.1.recv();
                    if !res.is_ok() {
                        info!("Exiting read loop for node/port {}/{}", node.id, receiver.0);
                        return;
                    }

                    let op = res.unwrap();
                    match op {
                        Operation::terminate => {
                            info!("Terminating read on node {} / port {}", node.id, receiver.0);
                            for mut e in &senders {
                                e.1.send(Operation::terminate);
                            }
                            return;
                        }
                        _ => {
                            info!("Incoming record on node {} / port {}", node.id, receiver.0);
                            let processed = node.processor.process((receiver.0, op), cloned_ctx.as_ref());
                            for rec in processed {
                                let sender = senders.get_mut(&rec.0);
                                if (sender.is_none()) {
                                    panic!("Unable to find output port {} in node {}", rec.0, node.id);
                                }
                                info!("Forwarding message from node {} / port {}", node.id, rec.0);
                                sender.unwrap().send(rec.1);

                            }
                        }
                    }

                }
            });
        }
        else {

            let mut m_node_senders : HashMap<u8, Arc<Mutex<Sender<Operation>>>> = HashMap::new();
            for mut t in node_senders {
                m_node_senders.insert(t.0, Arc::new(Mutex::new(t.1)));
            }

            let node_id = node.id;
            let mut m_node_processor = Arc::new(Mutex::new(node.processor));

            for mut receiver in node_receivers {

                let mut m_node_processor_clone = m_node_processor.clone();
                let mut m_node_senders_clone = m_node_senders.clone();
                let cloned_ctx = ctx.clone();

                let handle = thread::spawn( move || {
                    loop {

                        let res = receiver.1.recv();
                        if !res.is_ok() {
                            info!("Exiting read loop for node/port {}/{}", node_id, receiver.0);
                            return;
                        }

                        let op = res.unwrap();
                        match op {
                            Operation::terminate => {
                                info!("Terminating read on node {} / port {}", node_id, receiver.0);
                                for mut e in &m_node_senders_clone {
                                    e.1.lock().unwrap().send(Operation::terminate);
                                    return;
                                }
                            }
                            _ => {
                                info!("Incoming record on node {} / port {}", node_id, receiver.0);
                                let processed = m_node_processor_clone.lock().unwrap().process((receiver.0, op), cloned_ctx.as_ref());
                                for rec in processed {
                                    let sender = m_node_senders_clone.get_mut(&rec.0);
                                    if (sender.is_none()) {
                                        panic!("Unable to find output port {} in node {}", rec.0, node.id);
                                    }
                                    info!("Forwarding message from node {} / port {}", node.id, rec.0);
                                    sender.unwrap().lock().unwrap().send(rec.1);

                                }
                            }
                        }

                    }
                });
            }


        }

    }

    //handles.get(0).unwrap().

}