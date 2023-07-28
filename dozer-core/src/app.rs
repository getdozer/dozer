use dozer_types::node::NodeHandle;
use std::fmt::{Display, Formatter};

use crate::appsource::AppSourceManager;
use crate::errors::ExecutionError;
use crate::node::{PortHandle, ProcessorFactory, SinkFactory};
use crate::{Dag, Edge, Endpoint, DEFAULT_PORT_HANDLE};

use std::sync::Arc;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PipelineEntryPoint {
    source_name: String,
    port: PortHandle,
}

impl PipelineEntryPoint {
    pub fn new(source_name: String, port: PortHandle) -> Self {
        Self { source_name, port }
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }
}
#[derive(Clone, Debug)]
pub struct NamespacedEdge {
    edge: Edge,
    namespaced: bool,
}

impl Display for NamespacedEdge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            format!(
                "edge: [node: {}, port: {} -> node: {}, port: {}], , namespaced: {}",
                self.edge.from.node,
                self.edge.from.port,
                self.edge.to.node,
                self.edge.to.port,
                self.namespaced
            )
            .as_str(),
        )
    }
}

#[derive(Clone)]
pub struct AppPipeline<T> {
    edges: Vec<NamespacedEdge>,
    processors: Vec<(NodeHandle, Arc<dyn ProcessorFactory<T>>)>,
    sinks: Vec<(NodeHandle, Arc<dyn SinkFactory<T>>)>,
    entry_points: Vec<(NodeHandle, PipelineEntryPoint)>,
}

impl<T> Default for AppPipeline<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AppPipeline<T> {
    pub fn add_processor(
        &mut self,
        proc: Arc<dyn ProcessorFactory<T>>,
        id: &str,
        entry_point: Vec<PipelineEntryPoint>,
    ) {
        let handle = NodeHandle::new(None, id.to_string());
        self.processors.push((handle.clone(), proc.clone()));

        for p in entry_point {
            self.entry_points.push((handle.clone(), p));
        }
    }

    pub fn add_sink(&mut self, sink: Arc<dyn SinkFactory<T>>, id: &str) {
        let handle = NodeHandle::new(None, id.to_string());
        self.sinks.push((handle, sink));
    }

    pub fn connect_nodes(
        &mut self,
        from: &str,
        from_port: Option<PortHandle>,
        to: &str,
        to_port: Option<PortHandle>,
        namespaced: bool,
    ) {
        let edge = Edge::new(
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
        );
        self.edges.push(NamespacedEdge { edge, namespaced });
    }

    pub fn new() -> Self {
        Self {
            processors: Vec::new(),
            sinks: Vec::new(),
            edges: Vec::new(),
            entry_points: Vec::new(),
        }
    }

    pub fn get_entry_points_sources_names(&self) -> Vec<String> {
        self.entry_points
            .iter()
            .map(|(_, p)| p.source_name().to_string())
            .collect()
    }
}

pub struct App<T> {
    pipelines: Vec<(u16, AppPipeline<T>)>,
    app_counter: u16,
    sources: AppSourceManager<T>,
}

impl<T: Clone> App<T> {
    pub fn add_pipeline(&mut self, pipeline: AppPipeline<T>) {
        self.app_counter += 1;
        self.pipelines.push((self.app_counter, pipeline));
    }

    pub fn get_dag(&self) -> Result<Dag<T>, ExecutionError> {
        let mut dag = Dag::new();
        let mut entry_points: Vec<(String, Endpoint)> = Vec::new();

        for (connection, source) in self.sources.get_sources() {
            let node_handle = NodeHandle::new(None, connection.clone());
            dag.add_source(node_handle.clone(), source.clone());
        }

        for (pipeline_id, pipeline) in &self.pipelines {
            for (handle, proc) in &pipeline.processors {
                dag.add_processor(
                    NodeHandle::new(Some(*pipeline_id), handle.id.clone()),
                    proc.clone(),
                );
            }
            for (handle, sink) in &pipeline.sinks {
                dag.add_sink(
                    NodeHandle::new(Some(*pipeline_id), handle.id.clone()),
                    sink.clone(),
                );
            }
            for ne in &pipeline.edges {
                let edge = &ne.edge;
                let ns = if ne.namespaced {
                    Some(*pipeline_id)
                } else {
                    None
                };

                dag.connect(
                    Endpoint::new(
                        // NodeHandle::new(Some(*pipeline_id), edge.from.node.id.clone()),
                        NodeHandle::new(ns, edge.from.node.id.clone()),
                        edge.from.port,
                    ),
                    Endpoint::new(
                        NodeHandle::new(Some(*pipeline_id), edge.to.node.id.clone()),
                        edge.to.port,
                    ),
                )?;
            }

            for (handle, entry) in &pipeline.entry_points {
                entry_points.push((
                    entry.source_name.clone(),
                    Endpoint::new(
                        NodeHandle::new(Some(*pipeline_id), handle.id.clone()),
                        entry.port,
                    ),
                ));
            }
        }

        // Connect to all pipelines
        for (source_name, target_endpoint) in entry_points {
            let source_endpoint = self.sources.get_endpoint(&source_name)?;
            dag.connect(source_endpoint, target_endpoint)?;
        }

        Ok(dag)
    }

    pub fn new(sources: AppSourceManager<T>) -> Self {
        Self {
            pipelines: Vec::new(),
            app_counter: 0,
            sources,
        }
    }
}
