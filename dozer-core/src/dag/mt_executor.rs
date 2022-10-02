use crate::dag::dag::{Dag, Edge, Endpoint, NodeHandle, NodeType, PortDirection, PortHandle};
use crate::dag::node::{LocalChannelForwarder, ExecutionContext, NextStep, Processor, Sink, Source, ChannelForwarder, SourceFactory, ProcessorFactory, SinkFactory};
use dozer_types::types::{Operation, OperationEvent, Schema};

use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc};
use std::thread;
use std::thread::{JoinHandle};
use anyhow::{anyhow, Context};
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use crossbeam::select;
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::memory::MemoryStateStore;
use crate::state::StateStoresManager;

pub const DefaultPortHandle: u16 = 0xffff_u16;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SchemaKey {
    pub node_handle: NodeHandle,
    pub port_handle: PortHandle,
    pub direction: PortDirection
}

impl SchemaKey {
    pub fn new(node_handle: NodeHandle, port_handle: PortHandle, direction: PortDirection) -> Self {
        Self { node_handle, port_handle, direction }
    }
}

pub struct MultiThreadedDagExecutor {
    channel_buf_sz: usize
}

impl MultiThreadedDagExecutor {
    pub fn new(channel_buf_sz: usize) -> Self {
        Self { channel_buf_sz }
    }

    fn index_edges(
        &self, dag: &Dag
    ) -> (
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<OperationEvent>>>>,
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<OperationEvent>>>>
    ) {
        let mut senders: HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<OperationEvent>>>> =
            HashMap::new();
        let mut receivers: HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<OperationEvent>>>> =
            HashMap::new();

        for edge in dag.edges.iter() {
            if !senders.contains_key(&edge.from.node) {
                senders.insert(edge.from.node, HashMap::new());
            }
            if !receivers.contains_key(&edge.to.node) {
                receivers.insert(edge.to.node, HashMap::new());
            }

            let (tx, rx) = bounded(self.channel_buf_sz);

            let rcv_port: PortHandle = edge.to.port;
            if receivers
                .get(&edge.to.node)
                .unwrap()
                .contains_key(&rcv_port)
            {
                receivers
                    .get_mut(&edge.to.node)
                    .unwrap()
                    .get_mut(&rcv_port)
                    .unwrap()
                    .push(rx);
            } else {
                receivers
                    .get_mut(&edge.to.node)
                    .unwrap()
                    .insert(rcv_port, vec![rx]);
            }

            let snd_port: PortHandle = edge.from.port;
            if senders
                .get(&edge.from.node)
                .unwrap()
                .contains_key(&snd_port)
            {
                senders
                    .get_mut(&edge.from.node)
                    .unwrap()
                    .get_mut(&snd_port)
                    .unwrap()
                    .push(tx);
            } else {
                senders
                    .get_mut(&edge.from.node)
                    .unwrap()
                    .insert(snd_port, vec![tx]);
            }
        }

        (senders, receivers)
    }

    fn get_node_types(
        &self, dag: Dag
    ) -> (
        Vec<(NodeHandle, Box<dyn SourceFactory>)>,
        Vec<(NodeHandle, Box<dyn ProcessorFactory>)>,
        Vec<(NodeHandle, Box<dyn SinkFactory>)>,
    ) {
        let mut sources = Vec::new();
        let mut processors = Vec::new();
        let mut sinks = Vec::new();

        for node in dag.nodes.into_iter() {
            match node.1 {
                NodeType::Source(s) => sources.push((node.0, s)),
                NodeType::Processor(p) => {
                    processors.push((node.0, p));
                }
                NodeType::Sink(s) => {
                    sinks.push((node.0, s));
                }
            }
        }
        (sources, processors, sinks)
    }


    fn index_node_schemas(&self,
                          node_handle: NodeHandle,
                          node: &NodeType,
                          node_input_schemas: Option<HashMap<PortHandle, Schema>>,
                          dag: &Dag,
                          res: &mut HashMap<SchemaKey, Schema>
    ) -> anyhow::Result<()> {

        /// Get all output schemas of the current node
        let output_schemas = self.get_node_output_schemas(node, node_input_schemas)?;
        /// If no output schemas, it's a SNK so nothing to do
        if output_schemas.is_none() {
            return Ok(())
        }

        /// Iterate all output schemas for this node
        for s in output_schemas.unwrap() {

            /// Index (Node, Port, Output) -> Schema
            res.insert(SchemaKey::new(node_handle, s.0, PortDirection::Output), s.1.clone());

            /// Now get all edges connecting this (Node,Port) to the next nodes
            let out_edges : Vec<Endpoint> =
                dag.edges.iter()
                    .filter(|e| e.from.node == node_handle && e.from.port == s.0)
                    .map(|e| (e.to.clone()))
                    .collect();

            if out_edges.len() == 0 {
                return Err(anyhow!("The output port {} of the node {} is not connected", s.0, node_handle));
            }

            for out_e in out_edges {
                /// Register the target of the edge (Node, Port, Input) -> Schema
                res.insert(SchemaKey::new(out_e.node, out_e.port, PortDirection::Input), s.1.clone());

                /// find the next node in teh chain
                let next_node_handle = out_e.node;
                let next_node = dag.nodes.get(&out_e.node)
                    .context(anyhow!("Unable to find node {}", out_e.node))?;

                /// Get all input schemas for the next node
                let next_node_input_schemas = self.get_node_input_schemas(
                    next_node_handle, next_node, res
                );
                if next_node_input_schemas.is_none() {
                    return Ok(());
                }

                let r = self.index_node_schemas(
                    next_node_handle, next_node, next_node_input_schemas, dag, res
                );

                if r.is_err() {
                    return r;
                }

            }
        }
        Ok(())
    }

    fn get_node_output_schemas(&self, n: &NodeType, input_schemas: Option<HashMap<PortHandle, Schema>>) -> anyhow::Result<Option<HashMap<PortHandle, Schema>>> {

        match n {
            NodeType::Source(src) => {
                let mut schemas : HashMap<PortHandle, Schema> = HashMap::new();
                for p in src.get_output_ports() {
                    schemas.insert(p, src.get_output_schema(p)?);
                }
                Ok(Some(schemas))
            }
            NodeType::Processor(proc) => {
                let mut schemas : HashMap<PortHandle, Schema> = HashMap::new();
                for p in proc.get_output_ports() {
                    schemas.insert(p, proc.get_output_schema(p, input_schemas.clone().unwrap())?);
                }
                Ok(Some(schemas))
            }
            NodeType::Sink(snk) => { Ok(None) }
        }

    }

    fn get_node_input_schemas(
        &self, h: NodeHandle, n: &NodeType,
        res: &HashMap<SchemaKey, Schema>) -> Option<HashMap<PortHandle, Schema>> {

        match n {
            NodeType::Source(src) => {
                None
            }
            NodeType::Processor(proc) => {
                let mut schemas : HashMap<PortHandle, Schema> = HashMap::new();
                for p in proc.get_input_ports() {
                    let s = res.get(&SchemaKey::new(h, p, PortDirection::Input));
                    if s.is_none() { return None }
                    schemas.insert(p, s.unwrap().clone());
                }
                Some(schemas)
            }
            NodeType::Sink(snk) => {
                let mut schemas : HashMap<PortHandle, Schema> = HashMap::new();
                for p in snk.get_input_ports() {
                    let s = res.get(&SchemaKey::new(h, p, PortDirection::Input));
                    if s.is_none() { return None }
                    schemas.insert(p, s.unwrap().clone());
                }
                Some(schemas)
            }
        }

    }

    pub fn get_schemas_map(&self, dag: &Dag) -> anyhow::Result<HashMap<SchemaKey, Schema>> {


        let source: _ =
            dag.nodes.iter().find(|e| { matches!(e.1, NodeType::Source(_)) })
                .context("Unable to find a source node")?;

        let mut schemas: HashMap<SchemaKey, Schema> = HashMap::new();

        let r = self.index_node_schemas(
            *source.0, source.1, None, dag, &mut schemas
        )?;

        Ok(schemas)

    }

    fn start_source(
        &self, handle: NodeHandle,
        mut src_factory: Box<dyn SourceFactory>,
        senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
        state_manager: Arc<dyn StateStoresManager>
    ) -> JoinHandle<anyhow::Result<()>> {

        let local_sm = state_manager.clone();
        let fw = LocalChannelForwarder::new(senders);

        return thread::spawn(move || -> anyhow::Result<()> {

            let mut state_store = local_sm.init_state_store(handle.to_string())?;

            let mut src = src_factory.build();
            src.start(&fw, state_store.as_mut())

        });

    }

    fn build_receivers_lists(receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>)
        -> (Vec<PortHandle>, Vec<Receiver<OperationEvent>>)
    {
        let mut handles_ls : Vec<PortHandle> = Vec::new();
        let mut receivers_ls: Vec<Receiver<OperationEvent>> = Vec::new();
        for e in receivers {
            for r in e.1 {
                receivers_ls.push(r);
                handles_ls.push(e.0);
            }
        }
        (handles_ls, receivers_ls)
    }

    fn start_sink(
        &self, handle: NodeHandle,
        mut snk_factory: Box<dyn SinkFactory>,
        receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>,
        state_manager: Arc<dyn StateStoresManager>
    ) -> JoinHandle<anyhow::Result<()>> {

        let local_sm = state_manager.clone();
        thread::spawn(move || -> anyhow::Result<()> {

            let mut snk = snk_factory.build();
            let mut state_store = local_sm.init_state_store(handle.to_string())?;

            let (mut handles_ls, mut receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let mut sel = Select::new();
            for r in &receivers_ls { sel.recv(r); }
            loop {
                let index = sel.ready();
                let op = receivers_ls[index].recv()?;
                match op.operation {
                    Operation::Terminate => { return Ok(()); }
                    _ => {
                        let r = snk.process(
                            handles_ls[index],
                            op, state_store.as_mut()
                        )?;
                        match r {
                            NextStep::Stop => { return Ok(()); }
                            _ => { continue; }
                        }
                    }
                }
            }
        })

    }


    fn start_processor(
        &self, handle: NodeHandle,
        mut proc_factory: Box<dyn ProcessorFactory>,
        senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>,
        state_manager: Arc<dyn StateStoresManager>
    ) -> JoinHandle<anyhow::Result<()>> {

        let local_sm = state_manager.clone();
        thread::spawn(move || -> anyhow::Result<()> {

            let mut proc = proc_factory.build();
            let mut state_store = local_sm.init_state_store(handle.to_string())?;

            let (mut handles_ls, mut receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let fw = LocalChannelForwarder::new(senders);
            let mut sel = Select::new();
            for r in &receivers_ls { sel.recv(r); }

            proc.init(state_store.as_mut())?;
            loop {
                let index = sel.ready();
                let op = receivers_ls[index].recv()?;
                match op.operation {
                    Operation::Terminate => {
                        fw.terminate()?;
                        return Ok(());
                    }
                    _ => {
                        let r = proc.process(
                            handles_ls[index],
                            op, &fw, state_store.as_mut()
                        )?;
                        match r {
                            NextStep::Stop => { return Ok(()); }
                            _ => { continue; }
                        }
                    }
                }
            }
        })
    }

    pub fn start(&self, dag: Dag, state_manager: Arc<dyn StateStoresManager>) -> anyhow::Result<()> {

        let (mut senders, mut receivers) = self.index_edges(&dag);
        let (sources, processors, sinks) = self.get_node_types(dag);
        let mut handles: Vec<JoinHandle<anyhow::Result<()>>> = Vec::new();
        let global_sm = state_manager.clone();

        for snk in sinks {
            let snk_receivers = receivers.remove(&snk.0.clone());
            if snk_receivers.is_none() {
                return Err(anyhow!(
                    "The node {} does not have any input",
                    &snk.0.clone().to_string()
                ));
            }

            let snk_handle =
                self.start_sink(
                    snk.0, snk.1,
                    snk_receivers.unwrap(), global_sm.clone()
                );
            handles.push(snk_handle);

        }

        for processor in processors {

            let proc_receivers = receivers.remove(&processor.0.clone());
            if proc_receivers.is_none() {
                return Err(anyhow!("The node {} does not have any input", &processor.0.clone().to_string()));
            }

            let proc_senders = senders.remove(&processor.0.clone());
            if proc_senders.is_none() {
                return Err(anyhow!("The node {} does not have any output", &processor.0.clone().to_string()));
            }

            let proc_handle = self.start_processor(
                processor.0,
                processor.1,
                proc_senders.unwrap(),
                proc_receivers.unwrap(),
                global_sm.clone()
            );
            handles.push(proc_handle);

        }

        for source in sources {
            handles.push(
                self.start_source(
                    source.0, source.1,
                    senders.remove(&source.0.clone()).unwrap(),
                    global_sm.clone()
                ),
            );
        }

        for sh in handles {
            sh.join().unwrap()?;
        }
        
        Ok(())
    }
}


