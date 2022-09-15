use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::{JoinHandle, Thread};
use dozer_shared::types::{OperationEvent, Operation};
use crate::dag::channel::{LocalNodeChannel, NodeReceiver, NodeSender};
use crate::dag::dag::{Dag, Endpoint, NodeHandle, NodeType, PortHandle, TestProcessor, TestSink, TestSource};
use crate::dag::node::{ChannelForwarder, ExecutionContext, NextStep, Processor, ProcessorExecutor, Sink, SinkExecutor, Source};


pub struct MemoryExecutionContext {

}

impl MemoryExecutionContext {
    pub fn new() -> Self {
        Self {}
    }
}

impl ExecutionContext for MemoryExecutionContext {

}

pub const DEFAULT_PORT_ID : u8 = 0xffu8;

struct MultiThreadedDagExecutor {
    dag: Rc<Dag>
}


impl MultiThreadedDagExecutor {

    pub fn new(dag: Rc<Dag>) -> Self {
        Self { dag }
    }

    fn index_edges(&self) -> (
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Box<dyn NodeSender>>>>,
        HashMap<NodeHandle, HashMap<PortHandle, Vec<Box<dyn NodeReceiver>>>>
    ) {

        let mut senders : HashMap<NodeHandle, HashMap<PortHandle, Vec<Box<dyn NodeSender>>>> = HashMap::new();
        let mut receivers : HashMap<NodeHandle, HashMap<PortHandle, Vec<Box<dyn NodeReceiver>>>>  = HashMap::new();

        for edge in self.dag.edges.iter() {

            if !senders.contains_key(&edge.from.node) {
                senders.insert(edge.from.node, HashMap::new());
            }
            if !receivers.contains_key(&edge.to.node) {
                receivers.insert(edge.to.node, HashMap::new());
            }

            let (mut tx, mut rx) = edge.channel.build();

            let rcv_port: PortHandle = if (edge.to.port.is_none()) {DEFAULT_PORT_ID} else {edge.to.port.unwrap()};
            if receivers.get(&edge.to.node).unwrap().contains_key(&rcv_port) {
                let mut s = receivers.get_mut(&edge.to.node).unwrap().get_mut(&rcv_port).unwrap().push(rx);
            }
            else {
                receivers.get_mut(&edge.to.node).unwrap().insert(rcv_port, vec![rx]);
            }

            let snd_port: PortHandle = if (edge.from.port.is_none()) {DEFAULT_PORT_ID} else {edge.from.port.unwrap()};
            if senders.get(&edge.from.node).unwrap().contains_key(&snd_port) {
                let mut s = senders.get_mut(&edge.from.node).unwrap().get_mut(&snd_port).unwrap().push(tx);
            }
            else {
                senders.get_mut(&edge.from.node).unwrap().insert(snd_port, vec![tx]);
            }
        }

        (senders, receivers)
    }


    fn get_node_types(&self) -> (
        Vec<(NodeHandle, Arc<dyn Source>)>, Vec<(NodeHandle, Arc<dyn Processor>)>, Vec<(NodeHandle, Arc<dyn Sink>)>
    ) {

        let mut sources = Vec::new();
        let mut processors = Vec::new();
        let mut sinks = Vec::new();

        for node in self.dag.nodes.iter() {
            match node.1 {
                NodeType::Source(s) => { sources.push((node.0.clone(), s.clone())) }
                NodeType::Processor(p) => {  processors.push((node.0.clone(), p.clone())); }
                NodeType::Sink(s) => { sinks.push((node.0.clone(), s.clone())); }
            }
        }
        (sources, processors, sinks)
    }

    fn start_source(&self, src: Arc<dyn Source>, senders: HashMap<PortHandle, Vec<Box<dyn NodeSender>>>) -> JoinHandle<Result<(), String>> {

        let fw = ChannelForwarder::new(senders, false);
        return thread::spawn(move || -> Result<(), String> {
            src.start(&fw)
        });
    }

    fn start_sink(
        &self, snk: Arc<dyn Sink>,
        receivers: HashMap<PortHandle, Vec<Box<dyn NodeReceiver>>>,
        ctx: Arc<dyn ExecutionContext>
    ) -> Vec<JoinHandle<(Result<(), String>)>> {

        let receivers_count : i32 = receivers.values().map(|e| e.len() as i32).sum();
        let thread_safe = receivers_count > 1;
        let mut handles = Vec::new();

        let se = Arc::new(SinkExecutor::new(snk, thread_safe));

        for port_receivers in receivers {
            for receiver in port_receivers.1 {

                let local_ctx = ctx.clone();
                let local_se = se.clone();

                handles.push(thread::spawn(move || -> Result<(), String> {

                    loop {

                        let rcv = receiver.receive();
                        if rcv.is_err() {
                            return Err("Channel closed".to_string());;
                        }
                        let rcv_u = rcv.unwrap();
                        if matches!(rcv_u.operation, Operation::Terminate) {
                            return Ok(())
                        }

                        let res = local_se.process(
                            if port_receivers.0 == DEFAULT_PORT_ID { None } else { Some(port_receivers.0) },
                            rcv_u,
                            local_ctx.as_ref()
                        );
                        if res.is_err() {
                            return Err(format!("Sink returned an error: {}", res.err().unwrap()));
                        }
                        else if matches!(res.unwrap(), NextStep::Stop) {
                            return Ok(());
                        }
                    }
                }));
            }
        }

        handles
    }

    fn start_processor(
        &self,
        proc: Arc<dyn Processor>,
        mut senders: HashMap<PortHandle, Vec<Box<dyn NodeSender>>>,
        mut receivers: HashMap<PortHandle, Vec<Box<dyn NodeReceiver>>>,
        ctx: Arc<dyn ExecutionContext>
    ) -> Vec<JoinHandle<(Result<(), String>)>> {

        let receivers_count : i32 = receivers.values().map(|e| e.len() as i32).sum();
        let thread_safe = receivers_count > 1;
        let mut handles = Vec::new();

        let fw = Arc::new(ChannelForwarder::new(senders, thread_safe));
        let pe = Arc::new(ProcessorExecutor::new(proc, thread_safe));

        for port_receivers in receivers {
            for receiver in port_receivers.1 {

                let local_ctx = ctx.clone();
                let local_pe = pe.clone();
                let local_fw = fw.clone();

                handles.push(thread::spawn(move || -> Result<(), String> {

                    loop {
                        let rcv = receiver.receive();
                        if rcv.is_err() {
                            return Err("Channel closed".to_string());
                        }
                        let rcv_u = rcv.unwrap();
                        if matches!(rcv_u.operation, Operation::Terminate) {
                            local_fw.terminate()?
                        }

                        let res = local_pe.process(
                            if port_receivers.0 == DEFAULT_PORT_ID { None } else { Some(port_receivers.0) },
                            rcv_u,
                            local_ctx.as_ref(), local_fw.as_ref()
                        );
                        if res.is_err() {
                            return Err(format!("Processor returned an error: {}", res.err().unwrap()));
                        }
                        else if matches!(res.unwrap(), NextStep::Stop) {
                            return Ok(());
                        }
                    }
                }));
            }
        }

        handles
    }


    fn start(&self, ctx: Arc<dyn ExecutionContext>) -> Result<(), String>{

        let (mut senders, mut receivers) = self.index_edges();
        let (mut sources, mut processors, mut sinks) = self.get_node_types();

        for source in &sources {
            source.1.init();
        }
        for processor in &processors {
            processor.1.init();
        }
        for sink in &sinks {
            sink.1.init();
        }


        let mut source_handles: Vec<JoinHandle<Result<(), String>>> = Vec::new();
        let mut processor_handles: Vec<JoinHandle<Result<(), String>>> = Vec::new();
        let mut sink_handles: Vec<JoinHandle<Result<(), String>>> = Vec::new();

        for source in &sources {
            source_handles.push(self.start_source(
                source.1.clone(), senders.remove(&source.0.clone()).unwrap()
            ));
        }

        for processor in &processors {

            let proc_receivers = receivers.remove(&processor.0.clone());
            if (proc_receivers.is_none()) {
                return Err(format!("The node {} does not have any input", &processor.0.clone().to_string()));
            }
            let proc_senders = senders.remove(&processor.0.clone());
            if (proc_senders.is_none()) {
                return Err(format!("The node {} does not have any output", &processor.0.clone().to_string()));
            }

            let mut local_handles = self.start_processor(
                processor.1.clone(),
                proc_senders.unwrap(),
                proc_receivers.unwrap(),
                ctx.clone()
            );
            for h in local_handles { processor_handles.push(h); }
        }

        for snk in &sinks {

            let snk_receivers = receivers.remove(&snk.0.clone());
            if (snk_receivers.is_none()) {
                return Err(format!("The node {} does not have any input", &snk.0.clone().to_string()));
            }

            let mut local_handles = self.start_sink(
                snk.1.clone(),
                snk_receivers.unwrap(),
                ctx.clone()
            );
            for h in local_handles { sink_handles.push(h); }
        }

        for sh in source_handles {
            sh.join().unwrap()?
        }

        for sh in processor_handles {
            sh.join().unwrap()?
        }

        for sh in sink_handles {
            sh.join().unwrap()?
        }

        Ok(())

    }

}



#[test]
fn test_run_dag() {

    let src = TestSource::new(1,None);
    let proc = TestProcessor::new(1, None, None);
    let sink = TestSink::new(1,None);

    let mut dag = Dag::new();

    let src_handle = dag.add_node(NodeType::Source(Arc::new(src)));
    let proc_handle = dag.add_node(NodeType::Processor(Arc::new(proc)));
    let sink_handle = dag.add_node(NodeType::Sink(Arc::new(sink)));

    let src_to_proc1 = dag.connect(
        Endpoint::new(src_handle, None),
        Endpoint::new(proc_handle, None),
        Box::new(LocalNodeChannel::new(5000000))
    );

    let proc1_to_sink = dag.connect(
        Endpoint::new(proc_handle, None),
        Endpoint::new(sink_handle, None),
        Box::new(LocalNodeChannel::new(5000000))
    );

    let exec = MultiThreadedDagExecutor::new(Rc::new(dag));
    let ctx = Arc::new(MemoryExecutionContext::new());
    exec.start(ctx);

}

