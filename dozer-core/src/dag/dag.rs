use crate::dag::dag::PortDirection::{Input, Output};
use crate::dag::node::NextStep::Continue;
use crate::dag::node::{ChannelForwarder, ExecutionContext, NextStep, Processor, Sink, Source};
use dozer_types::types::{Operation, OperationEvent, Record};
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;
use uuid::Uuid;


pub type NodeHandle = Uuid;
pub type PortHandle = u8;

pub struct Endpoint {
    pub node: NodeHandle,
    pub port: Option<PortHandle>,
}

impl Endpoint {
    pub fn new(node: NodeHandle, port: Option<PortHandle>) -> Self {
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
    Source(Box<dyn Source>),
    Sink(Box<dyn Sink>),
    Processor(Box<dyn Processor>),
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

    pub fn add_node(&mut self, node_builder: NodeType) -> NodeHandle {
        let handle = Uuid::new_v4();
        self.nodes.insert(handle, node_builder);
        return handle;
    }

    fn check_port_for_node(
        &self,
        port: Option<PortHandle>,
        port_list: Option<Vec<PortHandle>>,
    ) -> Result<(), String> {
        if !port.is_none() {
            if port_list.is_none()
                || port_list
                    .unwrap()
                    .iter()
                    .find(|e| e == &&port.unwrap())
                    .is_none()
            {
                return Err(format!("Unable to find port {}", port.unwrap()));
            }
            return Ok(());
        } else {
            if !port_list.is_none() {
                return Err(format!("Node does not support default port"));
            }
            return Ok(());
        }
    }

    fn get_ports(&self, n: &NodeType, d: PortDirection) -> Result<Option<Vec<PortHandle>>, ()> {
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
                    Err(())
                } else {
                    Ok(s.get_input_ports())
                }
            }
            NodeType::Source(s) => {
                if matches!(d, Output) {
                    Ok(s.get_output_ports())
                } else {
                    Err(())
                }
            }
        }
    }

    pub fn connect(
        &mut self,
        from: Endpoint,
        to: Endpoint
    ) -> Result<(), String> {
        let src_node = self.nodes.get(&from.node);
        if src_node.is_none() {
            return Err(format!(
                "Unable to find source node with id = {}",
                from.node.to_string()
            ));
        }

        let dst_node = self.nodes.get(&to.node);
        if dst_node.is_none() {
            return Err(format!(
                "Unable to find source node with id = {}",
                to.node.to_string()
            ));
        }

        let src_output_ports = self.get_ports(src_node.unwrap(), Output);
        if src_output_ports.is_err() {
            return Err("The node type does not support output ports".to_string());
        }
        let res = self.check_port_for_node(from.port, src_output_ports.unwrap());
        if res.is_err() {
            return res;
        }

        let dst_input_ports = self.get_ports(dst_node.unwrap(), Input);
        if dst_input_ports.is_err() {
            return Err("The node type does not support input ports".to_string());
        }
        let res = self.check_port_for_node(to.port, dst_input_ports.unwrap());
        if res.is_err() {
            return res;
        }

        self.edges.push(Edge::new(from, to));

        Ok(())
    }
}


pub struct TestSink {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
}

impl TestSink {
    pub fn new(id: i32, input_ports: Option<Vec<PortHandle>>) -> Self {
        Self { id, input_ports }
    }
}

impl Sink for TestSink {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn init(&self) -> Result<(), String> {
        println!("SINK {}: Initialising TestSink", self.id);
        Ok(())
    }

    fn process(
        &self,
        _from_port: Option<PortHandle>,
        _op: OperationEvent,
        _ctx: &dyn ExecutionContext,
    ) -> Result<NextStep, String> {
         println!("SINK {}: Message {} received", self.id, _op.seq_no);
        Ok(Continue)
    }
}

pub struct TestProcessor {
    id: i32,
    input_ports: Option<Vec<PortHandle>>,
    output_ports: Option<Vec<PortHandle>>,
}

impl TestProcessor {
    pub fn new(
        id: i32,
        input_ports: Option<Vec<PortHandle>>,
        output_ports: Option<Vec<PortHandle>>,
    ) -> Self {
        Self {
            id,
            input_ports,
            output_ports,
        }
    }
}

impl Processor for TestProcessor {
    fn get_input_ports(&self) -> Option<Vec<PortHandle>> {
        self.input_ports.clone()
    }

    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        self.output_ports.clone()
    }

    fn init(&self) -> Result<(), String> {
        println!("PROC {}: Initialising TestProcessor", self.id);
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: Option<PortHandle>,
        op: OperationEvent,
        _ctx: &dyn ExecutionContext,
        fw: &dyn ChannelForwarder,
    ) -> Result<NextStep, String> {
        println!("PROC {}: Message {} received", self.id, op.seq_no);
        fw.send(op, None)?;
        Ok(Continue)
    }
}

pub struct TestSource {
    id: i32,
    output_ports: Option<Vec<PortHandle>>,
}

impl TestSource {
    pub fn new(id: i32, output_ports: Option<Vec<PortHandle>>) -> Self {
        Self { id, output_ports }
    }
}

impl Source for TestSource {
    fn get_output_ports(&self) -> Option<Vec<PortHandle>> {
        self.output_ports.clone()
    }

    fn init(&self) -> Result<(), String> {
        println!("SRC {}: Initialising TestProcessor", self.id);
        Ok(())
    }

    fn start(&self, fw: &dyn ChannelForwarder) -> Result<(), String> {
        for n in 0..1000 {
               println!("SRC {}: Message {} received", self.id, n);
            fw.send(
                OperationEvent::new(
                    n,
                    Operation::Insert {
                        new: Record::new(None, vec![]),
                    },
                ),
                None,
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
            let src = TestSource::new(1, $out_ports);
            let proc = TestProcessor::new(2, $in_ports, None);

            let mut dag = Dag::new();

            let src_handle = dag.add_node(NodeType::Source(Box::new(src)));
            let proc_handle = dag.add_node(NodeType::Processor(Box::new(proc)));

            let res = dag.connect(
                Endpoint::new(src_handle, $from_port),
                Endpoint::new(proc_handle, $to_port)
            );

            assert!(res.is_ok() == $expect)
        }
    };
}

test_ports!(test_none_ports, None, None, None, None, true);
test_ports!(
    test_matching_ports,
    Some(vec![1]),
    Some(vec![2]),
    Some(1),
    Some(2),
    true
);
test_ports!(
    test_not_matching_ports,
    Some(vec![2]),
    Some(vec![1]),
    Some(1),
    Some(2),
    false
);
test_ports!(
    test_not_default_port,
    Some(vec![2]),
    Some(vec![1]),
    None,
    Some(2),
    false
);
test_ports!(
    test_not_default_port2,
    None,
    Some(vec![1]),
    Some(1),
    Some(2),
    false
);
test_ports!(test_not_default_port3, None, None, None, Some(2), false);
