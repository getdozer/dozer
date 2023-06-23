use std::{collections::HashMap, sync::Arc};

use dozer_core::{
    app::{App, AppPipeline},
    appsource::{AppSource, AppSourceManager},
    node::{OutputPortDef, OutputPortType, PortHandle, SourceFactory},
    Dag,
};
use dozer_sql::pipeline::{
    builder::{statement_to_pipeline, SchemaSQLContext},
    errors::PipelineError,
};
use dozer_types::{
    grpc_types::cloud::{QueryEdge, QueryGraph, QueryNode, QueryNodeType},
    models::{app_config::Config, connection::Connection, source::Source},
};

use crate::pipeline::source_builder::SourceBuilder;

#[derive(Debug)]
struct UISourceFactory {
    output_ports: Vec<PortHandle>,
}
impl SourceFactory<SchemaSQLContext> for UISourceFactory {
    fn get_output_schema(
        &self,
        _port: &dozer_core::node::PortHandle,
    ) -> Result<
        (dozer_types::types::Schema, SchemaSQLContext),
        dozer_types::errors::internal::BoxedError,
    > {
        todo!()
    }

    fn get_output_ports(&self) -> Vec<dozer_core::node::OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortType::Stateless))
            .collect()
    }

    fn build(
        &self,
        _output_schemas: HashMap<dozer_core::node::PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn dozer_core::node::Source>, dozer_types::errors::internal::BoxedError> {
        todo!()
    }
}

fn prepare_pipeline_dag(
    sql: String,
    connection_sources: HashMap<Connection, Vec<Source>>,
    connection_source_ports: HashMap<(&str, &str), u16>,
) -> Result<Dag<SchemaSQLContext>, PipelineError> {
    let mut pipeline = AppPipeline::new();
    let mut asm: AppSourceManager<dozer_sql::pipeline::builder::SchemaSQLContext> =
        AppSourceManager::new();
    connection_sources.iter().for_each(|cs| {
        let (connection, sources) = cs;
        let ports = sources
            .iter()
            .map(|s| {
                let port = connection_source_ports
                    .get(&(connection.name.as_str(), s.name.as_str()))
                    .unwrap()
                    .to_owned();
                port
            })
            .collect();
        let mut ports_with_source_name: HashMap<String, u16> = HashMap::new();
        connection_source_ports.iter().for_each(|(k, v)| {
            ports_with_source_name.insert(k.1.to_string(), v.to_owned());
        });

        _ = asm.add(AppSource::new(
            connection.name.to_string(),
            Arc::new(UISourceFactory {
                output_ports: ports,
            }),
            ports_with_source_name,
        ));
    });
    statement_to_pipeline(&sql, &mut pipeline, None)?;
    let mut app = App::new(asm);
    app.add_pipeline(pipeline);
    let sql_dag = app.get_dag().unwrap();
    Ok(sql_dag)
}

fn transform_to_ui_graph(
    input_dag: Dag<SchemaSQLContext>,
    port_connection_source: HashMap<u16, (&str, &str)>,
) -> Result<QueryGraph, PipelineError> {
    let input_graph = input_dag.graph();
    let mut nodes = vec![];
    let mut edges: Vec<QueryEdge> = vec![];
    input_graph.raw_nodes().iter().enumerate().for_each(|n| {
        let weight = n.1.to_owned().weight;
        let idx = n.0;
        match weight.kind {
            dozer_core::NodeKind::Source(_) => {
                nodes.push(QueryNode {
                    name: weight.handle.id,
                    node_type: QueryNodeType::Connection as i32,
                    idx: idx as u32,
                    id: idx as u32,
                });
            }
            dozer_core::NodeKind::Processor(processor) => {
                nodes.push(QueryNode {
                    name: processor.type_name(),
                    node_type: QueryNodeType::Transformer as i32,
                    idx: idx as u32,
                    id: idx as u32,
                });
            }
            dozer_core::NodeKind::Sink(_) => {}
        }
    });
    input_graph.raw_edges().iter().for_each(|e| {
        let edge = e;
        let edge_type = edge.weight;
        let sn = edge.source();
        let tg = edge.target();
        let from = edge_type.from;
        let source_by_port = port_connection_source.get(&from);
        if let Some(source_by_port) = source_by_port {
            let source_name = source_by_port.1;
            let new_node_idx = (nodes.len()) as u32;
            nodes.push(QueryNode {
                name: source_name.to_string(),
                node_type: QueryNodeType::Source as i32,
                idx: new_node_idx,
                id: new_node_idx,
            });
            edges.push(QueryEdge {
                from: sn.index() as u32,
                to: new_node_idx,
            });
            edges.push(QueryEdge {
                from: new_node_idx,
                to: tg.index() as u32,
            });
        } else {
            edges.push(QueryEdge {
                from: sn.index() as u32,
                to: tg.index() as u32,
            });
        }
    });
    Ok(QueryGraph { nodes, edges })
}

pub fn config_to_ui_dag(config: Config) -> Result<QueryGraph, PipelineError> {
    let input_sources = config.sources;
    let sql = config.sql.unwrap_or("".to_string());
    let mut connection_sources: HashMap<Connection, Vec<Source>> = HashMap::new();
    input_sources.iter().for_each(|src| {
        let connection = src.connection.as_ref().unwrap().clone();
        let sources_same_connection = connection_sources.entry(connection).or_insert(vec![]);
        sources_same_connection.push(src.to_owned());
    });
    let source_builder = SourceBuilder::new(connection_sources.clone(), None);
    let connection_source_ports = source_builder.get_ports();
    let port_connection_source: HashMap<u16, (&str, &str)> = connection_source_ports
        .iter()
        .map(|(k, v)| (v.to_owned(), k.to_owned()))
        .collect();
    let sql_dag = prepare_pipeline_dag(sql, connection_sources, connection_source_ports)?;
    transform_to_ui_graph(sql_dag, port_connection_source)
}
