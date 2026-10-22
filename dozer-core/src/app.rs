use dozer_types::models::flags::{EnableProbabilisticOptimizations, Flags};
use dozer_types::node::NodeHandle;

use crate::appsource::{self, AppSourceManager};
use crate::errors::ExecutionError;
use crate::node::{PortHandle, ProcessorFactory, SinkFactory};
use crate::{Dag, Edge, Endpoint};

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
pub struct AppPipeline {
    edges: Vec<Edge>,
    processors: Vec<(NodeHandle, Box<dyn ProcessorFactory>)>,
    sinks: Vec<(NodeHandle, Box<dyn SinkFactory>)>,
    entry_points: Vec<(NodeHandle, PipelineEntryPoint)>,
    flags: PipelineFlags,
}

impl AppPipeline {
    fn create_handle(id: String) -> NodeHandle {
        NodeHandle::new(None, id)
    }

    pub fn add_processor(&mut self, proc: Box<dyn ProcessorFactory>, id: String) {
        self.processors.push((Self::create_handle(id), proc));
    }

    pub fn add_sink(&mut self, sink: Box<dyn SinkFactory>, id: String) {
        self.sinks.push((Self::create_handle(id), sink));
    }

    pub fn add_entry_point(&mut self, id: String, entry_point: PipelineEntryPoint) {
        self.entry_points
            .push((Self::create_handle(id), entry_point));
    }

    pub fn connect_nodes(
        &mut self,
        from: String,
        from_port: PortHandle,
        to: String,
        to_port: PortHandle,
    ) {
        let edge = Edge::new(
            Endpoint::new(NodeHandle::new(None, from), from_port),
            Endpoint::new(NodeHandle::new(None, to), to_port),
        );
        self.edges.push(edge);
    }

    pub fn new(flags: PipelineFlags) -> Self {
        Self {
            processors: Vec::new(),
            sinks: Vec::new(),
            edges: Vec::new(),
            entry_points: Vec::new(),
            flags,
        }
    }

    pub fn new_with_default_flags() -> Self {
        Self::new(Default::default())
    }

    pub fn get_entry_points_sources_names(&self) -> Vec<String> {
        self.entry_points
            .iter()
            .map(|(_, p)| p.source_name().to_string())
            .collect()
    }

    /// A checkpoint for removing unused processors added by one planner call.
    pub fn processor_count(&self) -> usize {
        self.processors.len()
    }

    /// Remove newly planned branches that cannot reach a declared output.
    /// Existing processors and all sinks are roots too, so unrelated pipeline
    /// contents and new ancestors that feed them are preserved.
    pub fn remove_unused_processors_since(&mut self, first_new: usize, outputs: &[String]) {
        use std::collections::HashSet;
        let mut live: HashSet<_> = self
            .processors
            .iter()
            .take(first_new)
            .map(|(handle, _)| handle.clone())
            .chain(self.sinks.iter().map(|(handle, _)| handle.clone()))
            .chain(outputs.iter().cloned().map(Self::create_handle))
            .collect();
        loop {
            let mut changed = false;
            for edge in &self.edges {
                if live.contains(&edge.to.node) {
                    changed |= live.insert(edge.from.node.clone());
                }
            }
            if !changed {
                break;
            }
        }
        let removed: HashSet<_> = self
            .processors
            .iter()
            .skip(first_new)
            .map(|(handle, _)| handle.clone())
            .filter(|handle| !live.contains(handle))
            .collect();
        self.processors
            .retain(|(handle, _)| !removed.contains(handle));
        self.edges
            .retain(|edge| !removed.contains(&edge.from.node) && !removed.contains(&edge.to.node));
        self.entry_points
            .retain(|(handle, _)| !removed.contains(handle));
    }

    pub fn flags(&self) -> &PipelineFlags {
        &self.flags
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineFlags {
    pub enable_probabilistic_optimizations: EnableProbabilisticOptimizations,
}

impl From<&Flags> for PipelineFlags {
    fn from(flags: &Flags) -> Self {
        Self {
            enable_probabilistic_optimizations: flags.enable_probabilistic_optimizations.clone(),
        }
    }
}

impl From<Flags> for PipelineFlags {
    fn from(flags: Flags) -> Self {
        Self::from(&flags)
    }
}

impl Default for PipelineFlags {
    fn default() -> Self {
        Flags::default().into()
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

    pub fn into_dag(self) -> Result<Dag, ExecutionError> {
        let mut dag = Dag::new();
        // (source name, target endpoint)
        let mut entry_points: Vec<(String, Endpoint)> = Vec::new();

        // Add all processors and sinks while collecting the entry points.
        for (pipeline_id, pipeline) in self.pipelines {
            for (handle, proc) in pipeline.processors {
                dag.add_processor(NodeHandle::new(Some(pipeline_id), handle.id), proc);
            }
            for (handle, sink) in pipeline.sinks {
                dag.add_sink(NodeHandle::new(Some(pipeline_id), handle.id), sink);
            }
            for edge in pipeline.edges {
                dag.connect(
                    Endpoint::new(
                        NodeHandle::new(Some(pipeline_id), edge.from.node.id),
                        edge.from.port,
                    ),
                    Endpoint::new(
                        NodeHandle::new(Some(pipeline_id), edge.to.node.id),
                        edge.to.port,
                    ),
                )?;
            }

            for (handle, entry) in pipeline.entry_points {
                entry_points.push((
                    entry.source_name,
                    Endpoint::new(NodeHandle::new(Some(pipeline_id), handle.id), entry.port),
                ));
            }
        }

        for (source, mapping) in self.sources.sources.into_iter().zip(&self.sources.mappings) {
            let node_handle = NodeHandle::new(None, mapping.connection.clone());
            dag.add_source(node_handle, source);
        }

        // Connect to all pipelines
        for (source_name, target_endpoint) in entry_points {
            let source_endpoint =
                appsource::get_endpoint_from_mappings(&self.sources.mappings, &source_name)?;
            dag.connect(source_endpoint, target_endpoint)?;
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

#[cfg(test)]
mod pruning_tests {
    use super::*;
    use crate::tests::{
        processors::ConnectivityTestProcessorFactory, sinks::ConnectivityTestSinkFactory,
    };

    #[test]
    fn retains_existing_nodes_outputs_sink_ancestors_and_prunes_only_unused_new_branches() {
        let mut pipeline = AppPipeline::new_with_default_flags();
        pipeline.add_processor(
            Box::new(ConnectivityTestProcessorFactory),
            "existing".into(),
        );
        let start = pipeline.processor_count();
        for name in [
            "used_from",
            "used_output",
            "dead_from",
            "dead_output",
            "sink_input",
            "existing_input",
        ] {
            pipeline.add_processor(Box::new(ConnectivityTestProcessorFactory), name.into());
        }
        pipeline.add_sink(Box::new(ConnectivityTestSinkFactory), "sink".into());
        for (from, to) in [
            ("used_from", "used_output"),
            ("dead_from", "dead_output"),
            ("sink_input", "sink"),
            ("existing_input", "existing"),
        ] {
            pipeline.connect_nodes(from.into(), 0, to.into(), 0);
        }
        for name in ["used_from", "dead_from", "sink_input", "existing_input"] {
            pipeline.add_entry_point(name.into(), PipelineEntryPoint::new(name.into(), 0));
        }
        pipeline.remove_unused_processors_since(start, &["used_output".into()]);
        let names = pipeline
            .processors
            .iter()
            .map(|(handle, _)| handle.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "existing",
                "used_from",
                "used_output",
                "sink_input",
                "existing_input"
            ]
        );
        assert_eq!(pipeline.edges.len(), 3);
        assert_eq!(
            pipeline.get_entry_points_sources_names(),
            vec!["used_from", "sink_input", "existing_input"]
        );
        assert_eq!(pipeline.sinks.len(), 1);
        pipeline.remove_unused_processors_since(start, &["used_output".into()]);
        assert_eq!(pipeline.processors.len(), 5);
    }
}
