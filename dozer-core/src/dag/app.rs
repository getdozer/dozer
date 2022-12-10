use crate::dag::dag::{Dag, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::node::{NodeHandle, PortHandle, ProcessorFactory, SinkFactory, SourceFactory};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct PipelineEntryPoint {
    id: String,
    port: Option<PortHandle>,
}

impl PipelineEntryPoint {
    pub fn new(id: String, port: Option<PortHandle>) -> Self {
        Self { id, port }
    }
}

pub struct ApplicationPipeline {
    dag: Dag,
    entry_points: Vec<PipelineEntryPoint>,
}

impl ApplicationPipeline {
    pub fn add_processor(
        &mut self,
        proc: Arc<dyn ProcessorFactory>,
        id: &str,
        entry_point: Option<PipelineEntryPoint>,
    ) {
        let handle = NodeHandle::new(None, id.to_string());
        self.dag.add_node(NodeType::Processor(proc), handle.clone());

        if let Some(e) = entry_point {
            self.entry_points.push(e);
        }
    }

    pub fn add_sink(&mut self, sink: Arc<dyn SinkFactory>, id: &str) {
        let handle = NodeHandle::new(None, id.to_string());
        self.dag.add_node(NodeType::Sink(sink), handle.clone());
    }

    pub fn connect_nodes(
        &mut self,
        from: &str,
        from_port: Option<PortHandle>,
        to: &str,
        to_port: Option<PortHandle>,
    ) -> Result<(), ExecutionError> {
        self.dag.connect(
            Endpoint::new(
                NodeHandle::new(None, from.to_string()),
                if let Some(port) = from_port {
                    port
                } else {
                    DEFAULT_PORT_HANDLE
                },
            ),
            Endpoint::new(
                NodeHandle::new(None, to.to_string()),
                if let Some(port) = to_port {
                    port
                } else {
                    DEFAULT_PORT_HANDLE
                },
            ),
        )
    }

    pub fn new() -> Self {
        Self {
            dag: Dag::new(),
            entry_points: Vec::new(),
        }
    }
}

// pub struct Application {
//     dag: Dag,
//     app_counter: u16,
// }
//
// impl Application {
//     pub fn add_source(&mut self, source: ApplicationSource) -> NodeHandle {
//         let handle = NodeHandle::new(None, id);
//         self.dag.add_node(NodeType::Source(src), handle.clone());
//         handle
//     }
//
//     pub fn add_pipeline(&mut self, app: ApplicationPipeline) {
//         self.app_counter += 1;
//         self.dag.merge(Some(self.app_counter), app.dag);
//
//         for e in app.entry_points {}
//     }
//
//     pub fn new() -> Self {
//         Self {
//             dag: Dag::new(),
//             app_counter: 0,
//         }
//     }
// }
