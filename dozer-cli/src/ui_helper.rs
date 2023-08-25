use std::collections::HashMap;

use dozer_core::{
    app::{App, AppPipeline},
    appsource::{AppSourceManager, AppSourceMappings},
    node::{OutputPortDef, OutputPortType, PortHandle, SourceFactory},
    petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeReferences},
    Dag,
};
use dozer_sql::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use dozer_types::{
    grpc_types::cloud::{QueryEdge, QueryGraph, QueryNode, QueryNodeType},
    models::{config::Config, connection::Connection, flags::Flags, source::Source},
};

use crate::{errors::OrchestrationError, pipeline::source_builder::SourceBuilder};

#[derive(Debug)]
struct UISourceFactory {
    output_ports: HashMap<PortHandle, String>,
}
impl SourceFactory<SchemaSQLContext> for UISourceFactory {
    fn get_output_schema(
        &self,
        _port: &PortHandle,
    ) -> Result<
        (dozer_types::types::Schema, SchemaSQLContext),
        dozer_types::errors::internal::BoxedError,
    > {
        todo!()
    }

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        self.output_ports.get(port).expect("Port not found").clone()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .keys()
            .map(|port| OutputPortDef::new(*port, OutputPortType::Stateless))
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
    flags: Flags,
) -> Result<Dag<SchemaSQLContext>, OrchestrationError> {
    let mut pipeline = AppPipeline::new(flags.into());
    let mut asm: AppSourceManager<dozer_sql::pipeline::builder::SchemaSQLContext> =
        AppSourceManager::new();
    connection_sources.iter().for_each(|cs| {
        let (connection, sources) = cs;
        let ports = sources
            .iter()
            .map(|source| {
                let port = *connection_source_ports
                    .get(&(connection.name.as_str(), source.name.as_str()))
                    .unwrap();
                (port, source.name.clone())
            })
            .collect();
        let mut ports_with_source_name: HashMap<String, u16> = HashMap::new();
        connection_source_ports.iter().for_each(|(k, v)| {
            ports_with_source_name.insert(k.1.to_string(), v.to_owned());
        });

        _ = asm.add(
            Box::new(UISourceFactory {
                output_ports: ports,
            }),
            AppSourceMappings::new(connection.name.to_string(), ports_with_source_name),
        );
    });
    statement_to_pipeline(&sql, &mut pipeline, None)?;
    let mut app = App::new(asm);
    app.add_pipeline(pipeline);
    let sql_dag = app.into_dag()?;
    Ok(sql_dag)
}

pub fn transform_to_ui_graph(input_dag: &Dag<SchemaSQLContext>) -> QueryGraph {
    let input_graph = input_dag.graph();
    let mut nodes = vec![];
    let mut edges: Vec<QueryEdge> = vec![];
    input_graph
        .node_references()
        .for_each(|(node_index, node)| {
            let node_index = node_index.index() as u32;
            match &node.kind {
                dozer_core::NodeKind::Source(_) => {
                    nodes.push(QueryNode {
                        name: node.handle.id.clone(),
                        node_type: QueryNodeType::Connection as i32,
                        idx: node_index,
                        id: node_index,
                        data: node.handle.id.clone(),
                    });
                }
                dozer_core::NodeKind::Processor(processor) => {
                    nodes.push(QueryNode {
                        name: processor.type_name(),
                        node_type: QueryNodeType::Transformer as i32,
                        idx: node_index,
                        id: node_index,
                        data: processor.id(),
                    });
                }
                dozer_core::NodeKind::Sink(_) => {}
            }
        });
    input_graph.edge_references().for_each(|edge| {
        let source_node_index = edge.source();
        let target_node_index = edge.target();
        let from_port = edge.weight().from;
        match &input_graph[source_node_index].kind {
            dozer_core::NodeKind::Source(source) => {
                let source_name = source.get_output_port_name(&from_port);
                let new_node_idx = nodes.len() as u32;
                nodes.push(QueryNode {
                    name: source_name.clone(),
                    node_type: QueryNodeType::Source as i32,
                    idx: new_node_idx,
                    id: new_node_idx,
                    data: source_name,
                });
                edges.push(QueryEdge {
                    from: source_node_index.index() as u32,
                    to: new_node_idx,
                });
                edges.push(QueryEdge {
                    from: new_node_idx,
                    to: target_node_index.index() as u32,
                });
            }
            _ => {
                edges.push(QueryEdge {
                    from: source_node_index.index() as u32,
                    to: target_node_index.index() as u32,
                });
            }
        }
    });
    QueryGraph { nodes, edges }
}

pub fn config_to_ui_dag(config: Config) -> Result<QueryGraph, OrchestrationError> {
    let sql = config.sql.unwrap_or("".to_string());
    let mut connection_sources: HashMap<Connection, Vec<Source>> = HashMap::new();
    for source in config.sources {
        let connection = config
            .connections
            .iter()
            .find(|connection| connection.name == source.connection)
            .cloned()
            .ok_or(OrchestrationError::ConnectionNotFound(
                source.connection.clone(),
            ))?;
        let sources_same_connection = connection_sources.entry(connection).or_insert(vec![]);
        sources_same_connection.push(source);
    }
    let source_builder = SourceBuilder::new(connection_sources.clone(), None);
    let connection_source_ports = source_builder.get_ports();
    let sql_dag = prepare_pipeline_dag(
        sql,
        connection_sources,
        connection_source_ports,
        config.flags.unwrap_or_default(),
    )?;
    Ok(transform_to_ui_graph(&sql_dag))
}
