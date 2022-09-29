use crate::dag::dag::{Dag, Endpoint, NodeHandle, NodeType, PortHandle, TestProcessor, TestSink, TestSource};
use crate::dag::node::{LocalChannelForwarder, ExecutionContext, NextStep, Processor, ProcessorExecutor, Sink, SinkExecutor, Source, ChannelForwarder};
use dozer_types::types::{Operation, OperationEvent};

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc};
use std::thread;
use std::thread::{JoinHandle};
use crossbeam::channel::{bounded, Receiver, Select, Sender};
use crossbeam::select;

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
    dag: Rc<Dag>,
    channel_buf_sz: usize
}

impl MultiThreadedDagExecutor {
    pub fn new(dag: Rc<Dag>, channel_buf_sz: usize) -> Self {
        Self { dag, channel_buf_sz }
    }

    fn index_edges(
        &self,
    ) -> (
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<OperationEvent>>>>,
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<OperationEvent>>>>
    ) {
        let mut senders: HashMap<NodeHandle, HashMap<PortHandle, Vec<Sender<OperationEvent>>>> =
            HashMap::new();
        let mut receivers: HashMap<NodeHandle, HashMap<PortHandle, Vec<Receiver<OperationEvent>>>> =
            HashMap::new();

        for edge in self.dag.edges.iter() {
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
        &self,
    ) -> (
        Vec<(NodeHandle, Arc<dyn Source>)>,
        Vec<(NodeHandle, Arc<dyn Processor>)>,
        Vec<(NodeHandle, Arc<dyn Sink>)>,
    ) {
        let mut sources = Vec::new();
        let mut processors = Vec::new();
        let mut sinks = Vec::new();

        for node in self.dag.nodes.iter() {
            match node.1 {
                NodeType::Source(s) => sources.push((node.0.clone(), s.clone())),
                NodeType::Processor(p) => {
                    processors.push((node.0.clone(), p.clone()));
                }
                NodeType::Sink(s) => {
                    sinks.push((node.0.clone(), s.clone()));
                }
            }
        }
        (sources, processors, sinks)
    }

    fn start_source(
        &self,
        src: Arc<dyn Source>,
        senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
    ) -> JoinHandle<Result<(), String>> {
        let fw = LocalChannelForwarder::new(senders);
        return thread::spawn(move || -> Result<(), String> { src.start(&fw) });
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
        &self,
        snk: Arc<dyn Sink>,
        receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>,
        ctx: Arc<dyn ExecutionContext>,
    ) -> JoinHandle<Result<(), String>> {

        thread::spawn(move || -> Result<(), String> {

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
                            op, ctx.as_ref()
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
        &self,
        proc: Arc<dyn Processor>,
        senders: HashMap<PortHandle, Vec<Sender<OperationEvent>>>,
        receivers: HashMap<PortHandle, Vec<Receiver<OperationEvent>>>,
        ctx: Arc<dyn ExecutionContext>,
    ) -> JoinHandle<Result<(), String>> {

        thread::spawn(move || -> Result<(), String> {

            let (mut handles_ls, mut receivers_ls) =
                MultiThreadedDagExecutor::build_receivers_lists(receivers);

            let fw = LocalChannelForwarder::new(senders);
            let mut sel = Select::new();
            for r in &receivers_ls { sel.recv(r); }
            loop {
                let index = sel.ready();
                let op = receivers_ls[index].recv().map_err(|e| {e.to_string()})?;
                match op.operation {
                    Operation::Terminate => { fw.terminate()?; }
                    _ => {
                        let r = proc.process(
                            if handles_ls[index] == DEFAULT_PORT_ID { None } else { Some(handles_ls[index]) },
                            op, ctx.as_ref(), &fw
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

    pub fn start(&self, ctx: Arc<dyn ExecutionContext>) -> Result<(), String> {
        let (mut senders, mut receivers) = self.index_edges();
        let (sources, processors, sinks) = self.get_node_types();

        for source in &sources {
            source.1.init()?;
        }
        for processor in &processors {
            processor.1.init()?;
        }
        for sink in &sinks {
            sink.1.init()?;
        }

        let mut source_handles: Vec<JoinHandle<Result<(), String>>> = Vec::new();
        let mut processor_handles: Vec<JoinHandle<Result<(), String>>> = Vec::new();
        let mut sink_handles: Vec<JoinHandle<Result<(), String>>> = Vec::new();

        for source in &sources {
            source_handles.push(
                self.start_source(source.1.clone(), senders.remove(&source.0.clone()).unwrap()),
            );
        }

        for processor in &processors {
            let proc_receivers = receivers.remove(&processor.0.clone());
            if proc_receivers.is_none() {
                return Err(format!(
                    "The node {} does not have any input",
                    &processor.0.clone().to_string()
                ));
            }
            let proc_senders = senders.remove(&processor.0.clone());
            if proc_senders.is_none() {
                return Err(format!(
                    "The node {} does not have any output",
                    &processor.0.clone().to_string()
                ));
            }

            let proc_handle = self.start_processor(
                processor.1.clone(),
                proc_senders.unwrap(),
                proc_receivers.unwrap(),
                ctx.clone(),
            );
            processor_handles.push(proc_handle);

        }

        for snk in &sinks {
            let snk_receivers = receivers.remove(&snk.0.clone());
            if snk_receivers.is_none() {
                return Err(format!(
                    "The node {} does not have any input",
                    &snk.0.clone().to_string()
                ));
            }

            let snk_handle =
                self.start_sink(snk.1.clone(), snk_receivers.unwrap(), ctx.clone());
            sink_handles.push(snk_handle);

        }

        for sh in source_handles {
            sh.join().unwrap();
        }

        for sh in processor_handles {
            sh.join().unwrap();
        }

        for sh in sink_handles {
            sh.join().unwrap();
        }

        Ok(())
    }
}

#[test]
fn test_run_dag() {
    let src = TestSource::new(1, None);
    let proc = TestProcessor::new(1, None, None);
    let sink = TestSink::new(1, None);

    let mut dag = Dag::new();

    let src_handle = dag.add_node(NodeType::Source(Arc::new(src)));
    let proc_handle = dag.add_node(NodeType::Processor(Arc::new(proc)));
    let sink_handle = dag.add_node(NodeType::Sink(Arc::new(sink)));

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

    let exec = MultiThreadedDagExecutor::new(Rc::new(dag), 100000);
    let ctx = Arc::new(MemoryExecutionContext::new());
    assert!(exec.start(ctx).is_ok());
}
