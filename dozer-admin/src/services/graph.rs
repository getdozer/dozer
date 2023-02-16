use std::collections::HashMap;

use crate::server::dozer_admin_grpc::{
    ErrorResponse, QueryEdge, QueryGraph, QueryNode, QueryNodeType,
};
use dozer_orchestrator::QueryContext;
use dozer_types::models::app_config::Config;

pub fn generate(context: Option<QueryContext>, cfg: &Config) -> Result<QueryGraph, ErrorResponse> {
    // output tables from query context
    let output_tables = match context {
        Some(context) => context.output_tables_map.keys().cloned().collect(),
        None => vec![],
    };

    let mut graph = QueryGraph::default();
    let mut nodes = vec![];
    let mut edges = vec![];

    let mut connection_map = HashMap::new();

    let mut source_map = HashMap::new();

    let mut id = 0;
    for (idx, source) in cfg.sources.iter().enumerate() {
        let connection_name = source.connection.as_ref().unwrap().name.clone();
        let c_id = connection_map.get(&connection_name);

        let c_id = match c_id {
            Some(id) => *id,
            None => {
                let c = cfg
                    .connections
                    .iter()
                    .enumerate()
                    .find(|(_idx, c)| c.name == connection_name);

                if let Some(c) = c {
                    id += 1;
                    nodes.push(QueryNode {
                        name: c.1.name.clone(),
                        node_type: QueryNodeType::Connection as i32,
                        idx: c.0 as u32,
                        id,
                    });

                    connection_map.insert(c.1.name.clone(), id);
                    id
                } else {
                    return Err(ErrorResponse {
                        message: format!("connection not found: {connection_name}"),
                    });
                }
            }
        };

        id += 1;
        nodes.push(QueryNode {
            name: source.name.clone(),
            node_type: QueryNodeType::Source as i32,
            idx: idx as u32,
            id,
        });

        source_map.insert(source.name.clone(), id);

        edges.push(QueryEdge {
            from: c_id,
            to: id,
            schema: None,
        });
    }
    const TRANSFORMER_ID: u32 = 10000;
    let mut transformed = false;
    for (idx, name) in output_tables.iter().enumerate() {
        nodes.push(QueryNode {
            name: name.clone(),
            node_type: QueryNodeType::Table as i32,
            idx: idx as u32,
            id,
        });
    }

    for (idx, endpoint) in cfg.endpoints.iter().enumerate() {
        id += 1;
        nodes.push(QueryNode {
            name: endpoint.name.clone(),
            node_type: QueryNodeType::Api as i32,
            idx: idx as u32,
            id,
        });
        let e_id = id;

        let s_id = source_map.get(&endpoint.table_name);
        let out_present = output_tables.contains(&endpoint.table_name);

        match (s_id, out_present) {
            (Some(s_id), _) => {
                edges.push(QueryEdge {
                    from: *s_id,
                    to: e_id,
                    schema: None,
                });
            }
            (None, true) => {
                id += 1;
                transformed = true;
                edges.push(QueryEdge {
                    from: TRANSFORMER_ID,
                    to: e_id,
                    schema: None,
                });
            }
            (None, false) => {
                return Err(ErrorResponse {
                    message: format!("table not found: {0}", endpoint.table_name),
                })
            }
        }
    }

    if transformed {
        nodes.push(QueryNode {
            name: "transformer".to_string(),
            node_type: QueryNodeType::Transformer as i32,
            idx: 0,
            id: TRANSFORMER_ID,
        });
    }

    graph.nodes = nodes;
    graph.edges = edges;
    Ok(graph)
}
