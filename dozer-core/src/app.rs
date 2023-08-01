use std::collections::HashMap;

use daggy::NodeIndex;
use dozer_types::node::NodeHandle;

use crate::appsource::{self, AppSourceManager};
use crate::errors::{ExecutionError, PipelineBuilderError};
use crate::node::{PortHandle, ProcessorFactory, SinkFactory};
use crate::Dag;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineEntryPoint {
    source_name: String,
    /// Target port.
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

#[derive(Debug)]
pub enum NodeType<T> {
    /// Dummy node that marks the entry point of a pipeline. Always created with an `EdgeType::EntryPoint` as its source node.
    Dummy,
    Processor {
        handle: NodeHandle,
        factory: Box<dyn ProcessorFactory<T>>,
    },
    Sink {
        handle: NodeHandle,
        factory: Box<dyn SinkFactory<T>>,
    },
}

#[derive(Debug)]
pub enum EdgeType {
    EntryPoint { source_name: String, to: PortHandle },
    Edge { from: PortHandle, to: PortHandle },
}

#[derive(Debug)]
pub struct AppPipeline<T> {
    graph: daggy::Dag<NodeType<T>, EdgeType>,
    node_name_to_index: HashMap<String, NodeIndex>,
}

impl<T> Default for AppPipeline<T> {
    fn default() -> Self {
        Self {
            graph: Default::default(),
            node_name_to_index: Default::default(),
        }
    }
}

impl<T> AppPipeline<T> {
    pub fn add_processor(
        &mut self,
        factory: Box<dyn ProcessorFactory<T>>,
        id: &str,
        entry_point: Vec<PipelineEntryPoint>,
    ) -> Result<NodeIndex, PipelineBuilderError> {
        // Add the processor.
        let handle = NodeHandle::new(None, id.to_string());
        let node_index = self.graph.add_node(NodeType::Processor { handle, factory });
        if self
            .node_name_to_index
            .insert(id.to_string(), node_index)
            .is_some()
        {
            return Err(PipelineBuilderError::DuplicateName(id.to_string()));
        }

        // Add the entry points.
        for entry_point in entry_point {
            self.add_entry_point(entry_point, node_index);
        }

        Ok(node_index)
    }

    pub fn add_sink(
        &mut self,
        sink: Box<dyn SinkFactory<T>>,
        id: &str,
        entry_point: Option<PipelineEntryPoint>,
    ) -> Result<NodeIndex, PipelineBuilderError> {
        // Add the sink.
        let handle = NodeHandle::new(None, id.to_string());
        let node_index = self.graph.add_node(NodeType::Sink {
            handle,
            factory: sink,
        });
        if self
            .node_name_to_index
            .insert(id.to_string(), node_index)
            .is_some()
        {
            return Err(PipelineBuilderError::DuplicateName(id.to_string()));
        }

        // Add the entry point.
        if let Some(entry_point) = entry_point {
            self.add_entry_point(entry_point, node_index);
        }

        Ok(node_index)
    }

    fn add_entry_point(&mut self, entry_point: PipelineEntryPoint, target_node_index: NodeIndex) {
        let dummy_node_index = self.graph.add_node(NodeType::Dummy);
        self.graph
            .add_edge(
                dummy_node_index,
                target_node_index,
                EdgeType::EntryPoint {
                    source_name: entry_point.source_name,
                    to: entry_point.port,
                },
            )
            .unwrap();
    }

    pub fn connect_nodes(
        &mut self,
        from: &str,
        from_port: PortHandle,
        to: &str,
        to_port: PortHandle,
    ) -> Result<(), PipelineBuilderError> {
        let from_node_index = self
            .node_name_to_index
            .get(from)
            .ok_or_else(|| PipelineBuilderError::NodeNameNotFound(from.to_string()))?;
        let to_node_index = self
            .node_name_to_index
            .get(to)
            .ok_or_else(|| PipelineBuilderError::NodeNameNotFound(to.to_string()))?;
        self.graph.add_edge(
            *from_node_index,
            *to_node_index,
            EdgeType::Edge {
                from: from_port,
                to: to_port,
            },
        )?;
        Ok(())
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn graph(&self) -> &daggy::Dag<NodeType<T>, EdgeType> {
        &self.graph
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

    pub fn into_dag(self) -> Result<Dag<T>, ExecutionError> {
        let mut dag = Dag::new();
        // (source name, target node index, target port)
        let mut entry_points: Vec<(String, NodeIndex, PortHandle)> = Vec::new();

        // Add all processors and sinks while collecting the entry points.
        for (pipeline_id, pipeline) in self.pipelines {
            let (nodes, edges) = pipeline.graph.into_graph().into_nodes_edges();
            let mut node_index_in_final_dag = vec![None; nodes.len()];

            // Add processors and sinks to final DAG.
            for (node, index) in nodes.into_iter().zip(node_index_in_final_dag.iter_mut()) {
                match node.weight {
                    NodeType::Processor {
                        mut handle,
                        factory,
                    } => {
                        handle.ns = Some(pipeline_id);
                        let node_index = dag.add_processor(handle, factory);
                        *index = Some(node_index);
                    }
                    NodeType::Sink {
                        mut handle,
                        factory,
                    } => {
                        handle.ns = Some(pipeline_id);
                        let node_index = dag.add_sink(handle, factory);
                        *index = Some(node_index);
                    }
                    NodeType::Dummy => {}
                }
            }

            // Traverse the edges.
            for edge in edges {
                let source = edge.source();
                let target = edge.target();
                match edge.weight {
                    EdgeType::EntryPoint { source_name, to } => {
                        // Record the entry point.
                        entry_points.push((
                            source_name,
                            node_index_in_final_dag[target.index()]
                                .expect("entry point edge must point to a processor or a sink"),
                            to,
                        ));
                    }
                    EdgeType::Edge { from, to } => {
                        // Connect the nodes.
                        let from_node_index = node_index_in_final_dag[source.index()]
                            .expect("edge must start from a processor");
                        let to_node_index = node_index_in_final_dag[target.index()]
                            .expect("edge must end at a processor or a sink");
                        dag.connect_with_index(from_node_index, from, to_node_index, to)?;
                    }
                }
            }
        }

        // Add all sources, remembering the node index for each connection name.
        let mut connection_name_to_node_index = HashMap::new();
        for (source, mapping) in self.sources.sources.into_iter().zip(&self.sources.mappings) {
            let node_handle = NodeHandle::new(None, mapping.connection.clone());
            let node_index = dag.add_source(node_handle, source);
            connection_name_to_node_index.insert(mapping.connection.clone(), node_index);
        }

        // Connect sources to entry points.
        for (source_name, target_node_index, target_port) in entry_points {
            let (connection_name, output_port) =
                appsource::get_endpoint_from_mappings(&self.sources.mappings, &source_name)?;
            let from_node_index = connection_name_to_node_index
                .get(connection_name)
                .expect("All connections has been added");
            dag.connect_with_index(
                *from_node_index,
                output_port,
                target_node_index,
                target_port,
            )?;
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
