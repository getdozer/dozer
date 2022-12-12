use crate::dag::appsource::{AppSource, AppSourceId, AppSourceManager};
use crate::dag::dag::{Dag, Edge, Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use crate::dag::errors::ExecutionError;
use crate::dag::node::{
    NodeHandle, PortHandle, Processor, ProcessorFactory, Sink, SinkFactory, SourceFactory,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct PipelineEntryPoint {
    id: AppSourceId,
    port: PortHandle,
}

impl PipelineEntryPoint {
    pub fn new(id: AppSourceId, port: PortHandle) -> Self {
        Self { id, port }
    }
}

pub struct AppPipeline {
    edges: Vec<Edge>,
    processors: Vec<(NodeHandle, Arc<dyn ProcessorFactory>)>,
    sinks: Vec<(NodeHandle, Arc<dyn SinkFactory>)>,
    entry_points: Vec<(NodeHandle, PipelineEntryPoint)>,
}

impl AppPipeline {
    pub fn add_processor(
        &mut self,
        proc: Arc<dyn ProcessorFactory>,
        id: &str,
        entry_point: Vec<PipelineEntryPoint>,
    ) {
        let handle = NodeHandle::new(None, id.to_string());
        self.processors.push((handle.clone(), proc.clone()));

        for p in entry_point {
            self.entry_points.push((handle.clone(), p));
        }
    }

    pub fn add_sink(&mut self, sink: Arc<dyn SinkFactory>, id: &str) {
        let handle = NodeHandle::new(None, id.to_string());
        self.sinks.push((handle, sink));
    }

    pub fn connect_nodes(
        &mut self,
        from: &str,
        from_port: Option<PortHandle>,
        to: &str,
        to_port: Option<PortHandle>,
    ) -> Result<(), ExecutionError> {
        Ok(self.edges.push(Edge::new(
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
        )))
    }

    pub fn new() -> Self {
        Self {
            processors: Vec::new(),
            sinks: Vec::new(),
            edges: Vec::new(),
            entry_points: Vec::new(),
        }
    }
}

pub struct App {
    pipelines: Vec<(u16, AppPipeline)>,
    app_counter: u16,
    sources: AppSourceManager,
}

impl App {
    pub fn add_pipeline(&mut self, pipeline: AppPipeline) {
        self.app_counter += 1;
        self.pipelines.push((self.app_counter, pipeline));
    }

    pub fn get_dag(&self) -> Result<Dag, ExecutionError> {
        let mut dag = Dag::new();
        let mut entry_points: Vec<(AppSourceId, Endpoint)> = Vec::new();

        for (pipeline_id, pipeline) in &self.pipelines {
            for (handle, proc) in &pipeline.processors {
                dag.add_node(
                    NodeType::Processor(proc.clone()),
                    NodeHandle::new(Some(pipeline_id.clone()), handle.id.clone()),
                );
            }
            for (handle, sink) in &pipeline.sinks {
                dag.add_node(
                    NodeType::Sink(sink.clone()),
                    NodeHandle::new(Some(pipeline_id.clone()), handle.id.clone()),
                );
            }
            for edge in &pipeline.edges {
                dag.connect(
                    Endpoint::new(
                        NodeHandle::new(Some(pipeline_id.clone()), edge.from.node.id.clone()),
                        edge.from.port,
                    ),
                    Endpoint::new(
                        NodeHandle::new(Some(pipeline_id.clone()), edge.to.node.id.clone()),
                        edge.to.port,
                    ),
                )?;
            }

            for (handle, entry) in &pipeline.entry_points {
                entry_points.push((
                    entry.id.clone(),
                    Endpoint::new(
                        NodeHandle::new(Some(pipeline_id.clone()), handle.id.clone()),
                        entry.port,
                    ),
                ));
            }
        }

        let mappings = self
            .sources
            .get(entry_points.iter().map(|e| e.0.clone()).collect())?;

        for mapping in &mappings {
            let node_handle = NodeHandle::new(None, mapping.source.connection.clone());
            dag.add_node(
                NodeType::Source(mapping.source.source.clone()),
                node_handle.clone(),
            );
            for entry in &entry_points {
                if let Some(e) = mapping.mappings.get(&entry.0) {
                    dag.connect(
                        Endpoint::new(node_handle.clone(), e.clone()),
                        entry.1.clone(),
                    )?;
                }
            }
        }

        Ok(dag)
    }

    pub fn new(sources: AppSourceManager) -> Self {
        Self {
            pipelines: Vec::new(),
            app_counter: 0,
            sources,
        }
    }
}
