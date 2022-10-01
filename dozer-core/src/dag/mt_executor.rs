use crate::dag::dag::{Dag, Endpoint, NodeHandle, NodeType, PortHandle, TestProcessor, TestProcessorFactory, TestSink, TestSinkFactory, TestSource, TestSourceFactory};
use crate::dag::node::{LocalChannelForwarder, ExecutionContext, NextStep, Processor, Sink, Source, ChannelForwarder, SourceFactory, ProcessorFactory, SinkFactory};
use dozer_types::types::{Operation, OperationEvent};

use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc};
use std::thread;
use std::thread::{JoinHandle};
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use crossbeam::select;
use crate::state::lmdb::LmdbStateStoreManager;
use crate::state::memory::MemoryStateStore;
use crate::state::StateStoresManager;

pub struct MemoryExecutionContext {
}

impl MemoryExecutionContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl ExecutionContext for MemoryExecutionContext {}

pub const DEFAULT_PORT_ID: u8 = 0xffu8;

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

            let rcv_port: PortHandle = if edge.to.port.is_none() {
                DEFAULT_PORT_ID
            } else {
                edge.to.port.unwrap()
            };
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

            let snd_port: PortHandle = if edge.from.port.is_none() {
                DEFAULT_PORT_ID
            } else {
                edge.from.port.unwrap()
            };
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

    fn start_source(
        &self, handle: NodeHandle,
        mut src_factory: Box<dyn SourceFactory>,
        senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
        state_manager: Arc<dyn StateStoresManager>
    ) -> JoinHandle<Result<(), String>> {

        let local_sm = state_manager.clone();
        let fw = LocalChannelForwarder::new(senders);

        return thread::spawn(move || -> Result<(), String> {

            let mut state_store = local_sm.init_state_store(handle.to_string())
                .map_err(|e| { e.desc })?;

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
    ) -> JoinHandle<Result<(), String>> {

        let local_sm = state_manager.clone();
        thread::spawn(move || -> Result<(), String> {

            let mut snk = snk_factory.build();
             let mut state_store = local_sm.init_state_store(handle.to_string())
                 .map_err(|e| { e.desc })?;

            let (mut handles_ls, mut receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let mut sel = Select::new();
            for r in &receivers_ls { sel.recv(r); }
            loop {
                let index = sel.ready();
                let op = receivers_ls[index].recv().map_err(|e| {e.to_string()})?;
                match op.operation {
                    Operation::Terminate => { return Ok(()); }
                    _ => {
                        let r = snk.process(
                            if handles_ls[index] == DEFAULT_PORT_ID { None } else { Some(handles_ls[index]) },
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
    ) -> JoinHandle<Result<(), String>> {

        let local_sm = state_manager.clone();
        thread::spawn(move || -> Result<(), String> {

            let mut proc = proc_factory.build();
            let mut state_store = local_sm.init_state_store(handle.to_string())
                .map_err(|e| { e.desc })?;

            let (mut handles_ls, mut receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let fw = LocalChannelForwarder::new(senders);
            let mut sel = Select::new();
            for r in &receivers_ls { sel.recv(r); }

            proc.init(state_store.as_mut())?;
            loop {
                let index = sel.ready();
                let op = receivers_ls[index].recv().map_err(|e| {e.to_string()})?;
                match op.operation {
                    Operation::Terminate => {
                        fw.terminate()?;
                        return Ok(());
                    }
                    _ => {
                        let r = proc.process(
                            if handles_ls[index] == DEFAULT_PORT_ID { None } else { Some(handles_ls[index]) },
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

    pub fn start(&self, dag: Dag, state_manager: Arc<dyn StateStoresManager>) -> Result<(), String> {

        let (mut senders, mut receivers) = self.index_edges(&dag);
        let (sources, processors, sinks) = self.get_node_types(dag);
        let mut handles: Vec<JoinHandle<Result<(), String>>> = Vec::new();
        let global_sm = state_manager.clone();

        for snk in sinks {
            let snk_receivers = receivers.remove(&snk.0.clone());
            if snk_receivers.is_none() {
                return Err(format!(
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
                return Err(format!("The node {} does not have any input", &processor.0.clone().to_string()));
            }

            let proc_senders = senders.remove(&processor.0.clone());
            if proc_senders.is_none() {
                return Err(format!("The node {} does not have any output", &processor.0.clone().to_string()));
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

#[test]
fn test_run_dag() {
    let src = TestSourceFactory::new(1, None);
    let proc = TestProcessorFactory::new(1, None, None);
    let sink = TestSinkFactory::new(1, None);

    let mut dag = Dag::new();

    let src_handle = dag.add_node(NodeType::Source(Box::new(src)));
    let proc_handle = dag.add_node(NodeType::Processor(Box::new(proc)));
    let sink_handle = dag.add_node(NodeType::Sink(Box::new(sink)));

    let src_to_proc1 = dag.connect(
        Endpoint::new(src_handle, None),
        Endpoint::new(proc_handle, None)
    );
    assert!(src_to_proc1.is_ok());

    let proc1_to_sink = dag.connect(
        Endpoint::new(proc_handle, None),
        Endpoint::new(sink_handle, None)
    );
    assert!(proc1_to_sink.is_ok());

    let exec = MultiThreadedDagExecutor::new( 100000);
    let ctx = Arc::new(MemoryExecutionContext::new());
    let sm = LmdbStateStoreManager::new("data".to_string(), 1024*1024*1024*5).unwrap();

    assert!(exec.start(dag, sm).is_ok());
}

