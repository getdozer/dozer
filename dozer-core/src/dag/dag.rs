use crate::dag::dag::PortDirection::{Input, Output};
use crate::dag::node::NextStep::Continue;
use crate::dag::node::{ChannelForwarder, ExecutionContext, NextStep, Processor, ProcessorFactory, Sink, SinkFactory, Source, SourceFactory};
use dozer_types::types::{Operation, OperationEvent, Record, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;
use anyhow::{anyhow, Error};
use uuid::Uuid;
use crate::dag::mt_executor::DefaultPortHandle;
use crate::state::{StateStore, StateStoresManager};


pub type NodeHandle = u16;
pub type PortHandle = u8;

pub struct Endpoint {
    pub node: NodeHandle,
    pub port: PortHandle,
}

impl Endpoint {
    pub fn new(node: NodeHandle, port: PortHandle) -> Self {
        Self { node, port }
    }
}

pub struct Edge {
    pub from: Endpoint,
    pub to: Endpoint
}

impl Edge {
    pub fn new(from: Endpoint, to: Endpoint) -> Self {
        Self { from, to }
    }
}

pub enum NodeType {
    Source(Box<dyn SourceFactory>),
    Sink(Box<dyn SinkFactory>),
    Processor(Box<dyn ProcessorFactory>),
}

pub struct Node {
    handle: NodeHandle,
    t: NodeType,
}


pub struct Dag {
    pub nodes: HashMap<NodeHandle, NodeType>,
    pub edges: Vec<Edge>,
}

enum PortDirection {
    Input,
    Output,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node_builder: NodeType, handle: NodeHandle) {
        self.nodes.insert(handle, node_builder);
    }

    fn get_ports(&self, n: &NodeType, d: PortDirection) -> anyhow::Result<Vec<PortHandle>> {
        match n {
            NodeType::Processor(p) => {
                if matches!(d, Output) {
                    Ok(p.get_output_ports())
                } else {
                    Ok(p.get_input_ports())
                }
            }
            NodeType::Sink(s) => {
                if matches!(d, Output) {
                    Err(anyhow!("Invalid node type"))
                } else {
                    Ok(s.get_input_ports())
                }
            }
            NodeType::Source(s) => {
                if matches!(d, Output) {
                    Ok(s.get_output_ports())
                } else {
                    Err(anyhow!("Invalid node type"))
                }
            }
        }
    }

    pub fn connect(
        &mut self,
        from: Endpoint,
        to: Endpoint
    ) -> anyhow::Result<()> {

        let src_node = self.nodes.get(&from.node);
        if src_node.is_none() {
            return Err(anyhow!(
                "Unable to find source node with id = {}",
                from.node.to_string()
            ));
        }

        let dst_node = self.nodes.get(&to.node);
        if dst_node.is_none() {
            return Err(anyhow!(
                "Unable to find source node with id = {}",
                to.node.to_string()
            ));
        }

        let src_output_ports = self.get_ports(src_node.unwrap(), Output)?;
        if !src_output_ports.contains(&from.port) {
            return Err(anyhow!("Unable to connect port"));
        }

        let dst_input_ports = self.get_ports(dst_node.unwrap(), Input)?;
        if !dst_input_ports.contains(&to.port) {
            return Err(anyhow!("Unable to connect port"));
        }

        self.edges.push(Edge::new(from, to));
        Ok(())
    }
}

pub struct TestSinkFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
}

impl TestSinkFactory {
    pub fn new(id: i32, input_ports: Vec<PortHandle>) -> Self {
        Self { id, input_ports }
    }
}


impl SinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn build(&self) -> Box<dyn Sink> {
        Box::new(TestSink { id: self.id })
    }
}


pub struct TestSink {
    id: i32
}

impl Sink for TestSink {
    
    fn init(&self, state_store: &mut dyn StateStore) -> anyhow::Result<()> {
        println!("SINK {}: Initialising TestSink", self.id);
        Ok(())
    }

    fn process(
        &self,
        _from_port: PortHandle,
        _op: OperationEvent,
        _state: &mut dyn StateStore
    ) -> anyhow::Result<NextStep> {
     //    println!("SINK {}: Message {} received", self.id, _op.seq_no);
        Ok(Continue)
    }
}

pub struct TestProcessorFactory {
    id: i32,
    input_ports: Vec<PortHandle>,
    output_ports: Vec<PortHandle>
}

impl TestProcessorFactory {
    pub fn new(id: i32, input_ports: Vec<PortHandle>, output_ports: Vec<PortHandle>) -> Self {
        Self { id, input_ports, output_ports }
    }
}

impl ProcessorFactory for TestProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(TestProcessor { state: None, id: self.id })
    }
}

pub struct TestProcessor {
    state: Option<Box<dyn StateStore>>,
    id: i32
}


impl Processor for TestProcessor {

    fn init<'a>(&'a mut self, state_store: &mut dyn StateStore) -> anyhow::Result<()> {
        println!("PROC {}: Initialising TestProcessor", self.id);
     //   self.state = Some(state_manager.init_state_store("pippo".to_string()).unwrap());
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: OperationEvent,
        fw: &dyn ChannelForwarder,
        state_store: &mut dyn StateStore
    ) -> anyhow::Result<NextStep> {
     //   println!("PROC {}: Message {} received", self.id, op.seq_no);
        state_store.put(&op.seq_no.to_ne_bytes(), &self.id.to_ne_bytes());
        fw.send(op, DefaultPortHandle)?;
        Ok(Continue)
    }
}

pub struct TestSourceFactory {
    id: i32,
    output_ports: Vec<PortHandle>
}

impl TestSourceFactory {
    pub fn new(id: i32, output_ports: Vec<PortHandle>) -> Self {
        Self { id, output_ports }
    }
}

impl SourceFactory for TestSourceFactory {

    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }

    fn build(&self) -> Box<dyn Source> {
        Box::new(TestSource {id: self.id})
    }
}

pub struct TestSource {
    id: i32
}

impl Source for TestSource {

    fn start(&self, fw: &dyn ChannelForwarder, state: &mut dyn StateStore) -> anyhow::Result<()> {
        for n in 0..10000000 {
             //  println!("SRC {}: Message {} received", self.id, n);
            fw.send(
                OperationEvent::new(
                    n,
                    Operation::Insert {
                        new: Record::new(None, vec![]),
                    },
                ),
                DefaultPortHandle,
            )
            .unwrap();
        }
        fw.terminate().unwrap();
        Ok(())
    }
}

macro_rules! test_ports {
    ($id:ident, $out_ports:expr, $in_ports:expr, $from_port:expr, $to_port:expr, $expect:expr) => {
        #[test]
        fn $id() {
            let src = TestSourceFactory::new(1, $out_ports);
            let proc = TestProcessorFactory::new(2, $in_ports, vec![DefaultPortHandle]);

            let mut dag = Dag::new();

            dag.add_node(NodeType::Source(Box::new(src)), 1);
            dag.add_node(NodeType::Processor(Box::new(proc)), 2);

            let res = dag.connect(
                Endpoint::new(1, $from_port),
                Endpoint::new(2, $to_port)

            );

            assert!(res.is_ok() == $expect)
        }
    };
}

test_ports!(
    test_none_ports,
    vec![DefaultPortHandle],
    vec![DefaultPortHandle],
    DefaultPortHandle,
    DefaultPortHandle,
    true);

test_ports!(
    test_matching_ports,
    vec![1],
    vec![2],
    1,
    2,
    true
);
test_ports!(
    test_not_matching_ports,
    vec![2],
    vec![1],
    1,
    2,
    false
);
test_ports!(
    test_not_default_port,
    vec![2],
    vec![1],
    DefaultPortHandle,
    2,
    false
);
test_ports!(
    test_not_default_port2,
    vec![DefaultPortHandle],
    vec![1],
    1,
    2,
    false
);
test_ports!(
    test_not_default_port3,
    vec![DefaultPortHandle],
    vec![DefaultPortHandle],
    DefaultPortHandle,
    2,
    false
);
