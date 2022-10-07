use crate::dag::dag::{Dag, Edge, Endpoint, NodeHandle, NodeType, PortDirection, PortHandle};
use crate::dag::node::{
    ExecutionContext, NextStep, Processor, ProcessorFactory, Sink, SinkFactory, Source,
    SourceFactory,
};
use dozer_types::types::{Operation, OperationEvent, Schema};

use crate::dag::forwarder::{LocalChannelForwarder, SourceChannelForwarder};
use crate::state::StateStoresManager;
use anyhow::anyhow;
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use log::{error, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub const DefaultPortHandle: u16 = 0xffff_u16;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SchemaKey {
    pub node_handle: NodeHandle,
    pub port_handle: PortHandle,
    pub direction: PortDirection,
}

impl SchemaKey {
    pub fn new(node_handle: NodeHandle, port_handle: PortHandle, direction: PortDirection) -> Self {
        Self {
            node_handle,
            port_handle,
            direction,
        }
    }
}

pub struct MultiThreadedDagExecutor {
    channel_buf_sz: usize,
}

impl MultiThreadedDagExecutor {
    pub fn new(channel_buf_sz: usize) -> Self {
        Self { channel_buf_sz }
    }

    fn index_edges(
        &self,
        dag: &Dag,
    ) -> (
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<OperationEvent>>>>,
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<OperationEvent>>>>,
    ) {
        let mut senders: HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<OperationEvent>>>> =
            HashMap::new();
        let mut receivers: HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<OperationEvent>>>> =
            HashMap::new();

        for edge in dag.edges.iter() {
            if !senders.contains_key(&edge.from.node) {
                senders.insert(edge.from.node.clone(), HashMap::new());
            }
            if !receivers.contains_key(&edge.to.node) {
                receivers.insert(edge.to.node.clone(), HashMap::new());
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
        &self,
        dag: Dag,
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

    fn start_source(
        &self,
        handle: NodeHandle,
        mut src_factory: Box<dyn SourceFactory>,
        senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
        state_manager: Arc<dyn StateStoresManager>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let local_sm = state_manager.clone();
        let fw = LocalChannelForwarder::new(senders);

        thread::spawn(move || -> anyhow::Result<()> {
            let mut state_store = local_sm.init_state_store(handle.to_string())?;

            let mut src = src_factory.build();
            for p in src_factory.get_output_ports() {
                let schema = src.get_output_schema(p);
                fw.update_schema(schema, p)?
            }
            src.start(&fw, &fw, state_store.as_mut(), None)
        })
    }

    fn build_receivers_lists(
        receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>,
    ) -> (Vec<PortHandle>, Vec<Receiver<OperationEvent>>) {
        let mut handles_ls: Vec<PortHandle> = Vec::new();
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
        &self,
        handle: NodeHandle,
        mut snk_factory: Box<dyn SinkFactory>,
        receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>,
        state_manager: Arc<dyn StateStoresManager>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let local_sm = state_manager.clone();
        thread::spawn(move || -> anyhow::Result<()> {
            let mut snk = snk_factory.build();
            let mut state_store = local_sm.init_state_store(handle.to_string())?;

            let (mut handles_ls, mut receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            snk.init(state_store.as_mut())?;

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            let mut sel = Select::new();
            for r in &receivers_ls {
                sel.recv(r);
            }
            loop {
                let index = sel.ready();
                let op = receivers_ls[index].recv()?;
                match op.operation {
                    Operation::SchemaUpdate { new } => {
                        input_schemas.insert(handles_ls[index], new);
                        let input_ports = snk_factory.get_input_ports();
                        let count: Vec<&PortHandle> = input_ports
                            .iter()
                            .filter(|e| !input_schemas.contains_key(*e))
                            .collect();
                        if count.len() == 0 {
                            let r = snk.update_schema(&input_schemas);
                            if r.is_ok() {
                                schema_initialized = true;
                            } else {
                                warn!(
                                    "New schema is not compatible with older version. Handling it."
                                );
                                todo!("Schema is not compatible with order version. Handle it!")
                            }
                        }
                    }

                    Operation::Terminate => {
                        return Ok(());
                    }

                    _ => {
                        if !schema_initialized {
                            error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
                            return Err(anyhow!("Received a CDC before schema initialization"));
                        }

                        let r = snk.process(handles_ls[index], op, state_store.as_mut())?;
                        match r {
                            NextStep::Stop => {
                                return Ok(());
                            }
                            _ => {
                                continue;
                            }
                        }
                    }
                }
            }
        })
    }

    fn start_processor(
        &self,
        handle: NodeHandle,
        mut proc_factory: Box<dyn ProcessorFactory>,
        senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>,
        state_manager: Arc<dyn StateStoresManager>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let local_sm = state_manager.clone();
        thread::spawn(move || -> anyhow::Result<()> {
            let mut proc = proc_factory.build();
            let mut state_store = local_sm.init_state_store(handle.to_string())?;

            let (mut handles_ls, mut receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let mut fw = LocalChannelForwarder::new(senders);
            let mut sel = Select::new();
            for r in &receivers_ls {
                sel.recv(r);
            }

            let mut input_schemas = HashMap::<PortHandle, Schema>::new();
            let mut output_schemas = HashMap::<PortHandle, Schema>::new();
            let mut schema_initialized = false;

            proc.init(state_store.as_mut())?;
            loop {
                let index = sel.ready();
                let op = receivers_ls[index].recv()?;
                match op.operation {
                    Operation::SchemaUpdate { new } => {
                        input_schemas.insert(handles_ls[index], new);
                        let input_ports = proc_factory.get_input_ports();
                        let count: Vec<&PortHandle> = input_ports
                            .iter()
                            .filter(|e| !input_schemas.contains_key(*e))
                            .collect();
                        if count.len() == 0 {
                            for out_port in proc_factory.get_output_ports() {
                                let r = proc.update_schema(out_port, &input_schemas);
                                if r.is_ok() {
                                    let out_schema = r.unwrap();
                                    output_schemas.insert(out_port, out_schema.clone());
                                    fw.update_schema(out_schema, out_port)?;
                                    schema_initialized = true;
                                } else {
                                    warn!("New schema is not compatible with older version. Handling it.");
                                    todo!("Schema is not compatible with order version. Handle it!")
                                }
                            }
                        }
                    }
                    Operation::Terminate => {
                        fw.terminate()?;
                        return Ok(());
                    }
                    _ => {
                        if !schema_initialized {
                            error!("Received a CDC before schema initialization. Exiting from SNK message loop.");
                            return Err(anyhow!("Received a CDC before schema initialization"));
                        }
                        fw.update_seq_no(op.seq_no);
                        let r = proc.process(
                            handles_ls[index],
                            op.operation,
                            &fw,
                            state_store.as_mut(),
                        )?;
                        match r {
                            NextStep::Stop => {
                                return Ok(());
                            }
                            _ => {
                                continue;
                            }
                        }
                    }
                }
            }
        })
    }

    pub fn start(
        &self,
        dag: Dag,
        state_manager: Arc<dyn StateStoresManager>,
    ) -> anyhow::Result<()> {
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
                self.start_sink(snk.0, snk.1, snk_receivers.unwrap(), global_sm.clone());
            handles.push(snk_handle);
        }

        for processor in processors {
            let proc_receivers = receivers.remove(&processor.0.clone());
            if proc_receivers.is_none() {
                return Err(anyhow!(
                    "The node {} does not have any input",
                    &processor.0.clone().to_string()
                ));
            }

            let proc_senders = senders.remove(&processor.0.clone());
            if proc_senders.is_none() {
                return Err(anyhow!(
                    "The node {} does not have any output",
                    &processor.0.clone().to_string()
                ));
            }

            let proc_handle = self.start_processor(
                processor.0,
                processor.1,
                proc_senders.unwrap(),
                proc_receivers.unwrap(),
                global_sm.clone(),
            );
            handles.push(proc_handle);
        }

        for source in sources {
            handles.push(self.start_source(
                source.0.clone(),
                source.1,
                senders.remove(&source.0.clone()).unwrap(),
                global_sm.clone(),
            ));
        }

        for sh in handles {
            sh.join().unwrap()?;
        }

        Ok(())
    }
}
