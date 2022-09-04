// use std::collections::HashMap;
// use std::ops::Deref;
// use std::rc::Rc;
// use std::sync::Arc;
// use tokio::sync::mpsc;
// use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
// use tokio::task::JoinHandle;
// use crate::executor::{Executor, Pipe};
// use crate::{ExecutionContext, Processor, Record};
//
//
// pub struct DagExecutor {
//     processors: HashMap<u16, Box<dyn Processor>>,
//     pipes: Vec<(u32,u32)>,
//     counter: u16,
//     senders: HashMap<u32, UnboundedSender<Record>>,
//     receivers: Vec<(UnboundedReceiver<Record>, u32)>
// }
//
// impl DagExecutor {
//     pub fn new() -> DagExecutor {
//         DagExecutor {
//             processors: HashMap::new(),
//             pipes: Vec::new(),
//             counter: 0,
//             senders: HashMap::new(),
//             receivers: Vec::new()
//         }
//     }
// }
//
//
// impl Executor for DagExecutor {
//
//     fn register_processor(&mut self, processor: Box<dyn Processor>) -> u16 {
//         self.counter +=1;
//         self.processors.insert(self.counter, processor);
//         self.counter
//     }
//
//     fn register_pipe(&mut self, pipe: Pipe) {
//         let from_node_port : u32 = u32::from(pipe.from) << 16 | u32::from(pipe.from_port);
//         let to_node_port : u32 = u32::from(pipe.to) << 16 | u32::from(pipe.to_port);
//         self.pipes.push((from_node_port, to_node_port));
//     }
//
//     fn register_input(&mut self, rx: UnboundedReceiver<Record>, node: u16, port: u8) {
//         let to_node_port : u32 = u32::from(node) << 16 | u32::from(port);
//         self.receivers.push((rx, to_node_port))
//     }
//
//     fn register_output(&mut self, node: u16, port: u8, tx: UnboundedSender<Record>) {
//         let key : u32 = u32::from(node) << 16 | u32::from(port);
//         self.senders.insert(key, tx);
//     }
//
//     fn prepare(&mut self, context: &ExecutionContext) {
//
//         for p in &self.pipes {
//             let (mut tx, mut rx) = mpsc::unbounded_channel::<Record>();
//             self.senders.insert(p.0, tx);
//             self.receivers.push((rx, p.1));
//         }
//
//         for p in self.processors.values() {
//             p.prepare(context);
//         }
//     }
//
//     fn start(&mut self, context: &ExecutionContext) -> Vec<JoinHandle<()>> {
//
//         let mut handles = Vec::new();
//
//         for receiver in &self.receivers {
//             let arc_receiver = Arc::new(receiver);
//             handles.push(tokio::spawn(async move {
//                 loop {
//                     let result = arc_receiver.0.recv().await;
//                     if result.is_none() {
//                         break;
//                     }
//                     println!("received")
//                 }
//             }));
//         }
//
//         handles
//
//     }
//
//     fn stop(&mut self, context: &ExecutionContext) {
//         todo!()
//     }
// }