use crate::dag_schemas::{DagHaveSchemas, DagSchemas, EdgeType};
use crate::errors::{ExecutionError, IncompatibleSchemas};
use crate::node::PortHandle;
use crate::NodeKind;
use daggy::petgraph::visit::{EdgeRef, IntoEdgesDirected, IntoNodeReferences, Topo};
use daggy::petgraph::Direction;
use daggy::{NodeIndex, Walker};
use dozer_storage::common::Seek;
use dozer_storage::errors::StorageError;
use dozer_storage::errors::StorageError::{DeserializationError, SerializationError};
use dozer_storage::lmdb::{Database, DatabaseFlags};
use dozer_storage::lmdb_storage::{
    LmdbEnvironmentManager, LmdbEnvironmentOptions, LmdbExclusiveTransaction, SharedTransaction,
};
use dozer_types::bincode;
use dozer_types::log::debug;
use dozer_types::node::{NodeHandle, OpIdentifier, SourceStates};
use dozer_types::types::Schema;
use std::collections::HashMap;

use std::path::PathBuf;

pub(crate) const METADATA_DB_NAME: &str = "__META__";
const SOURCE_ID_IDENTIFIER: u8 = 0_u8;
pub(crate) const OUTPUT_SCHEMA_IDENTIFIER: u8 = 1_u8;
pub(crate) const INPUT_SCHEMA_IDENTIFIER: u8 = 2_u8;

#[derive(Debug, Clone)]
/// A node's storage environment.
pub struct NodeStorage {
    /// The transaction to read from and write to the storage.
    pub master_txn: SharedTransaction,
    /// The metadata database. Including schemas and checkpoints.
    pub meta_db: Database,
}

/// Node type for `DagMetadata`.
pub struct NodeType<T> {
    /// Name of the node.
    pub handle: NodeHandle,
    /// Checkpoint read from the storage, or empty if the storage isn't created yet.
    pub commits: SourceStates,
    /// The node's storage.
    pub storage: Option<NodeStorage>,
    /// The node kind.
    pub kind: NodeKind<T>,
}

/// `DagMetadata` reads the metadata information from disk and attach it to node.
///
/// It also checks if the persisted schemas are the same with current schemas.
pub struct DagMetadata<T> {
    /// Base path of all node storages.
    path: PathBuf,
    graph: daggy::Dag<NodeType<T>, EdgeType>,
}

impl<T> DagHaveSchemas for DagMetadata<T> {
    type NodeType = NodeType<T>;
    type EdgeType = EdgeType;

    fn graph(&self) -> &daggy::Dag<Self::NodeType, Self::EdgeType> {
        &self.graph
    }
}

impl<T> DagMetadata<T> {
    pub fn into_graph(self) -> daggy::Dag<NodeType<T>, EdgeType> {
        self.graph
    }

    /// Returns `Ok` if validation passes, `Err` otherwise.
    pub fn new(dag_schemas: DagSchemas<T>, path: PathBuf) -> Result<Self, ExecutionError> {
        // Load node metadata.
        let mut metadata = vec![];
        for (node_index, node) in dag_schemas.graph().node_references() {
            let env_name = node_environment_name(&node.handle);

            // Return empty metadata if env doesn't exist.
            if !LmdbEnvironmentManager::exists(&path, &env_name) {
                debug!(
                    "[checkpoint] Node [{}] is at checkpoint {:?}",
                    node.handle,
                    SourceStates::new()
                );
                metadata.push(Some((None, SourceStates::new())));
                continue;
            }

            // Open environment and database.
            let mut env = LmdbEnvironmentManager::create(
                &path,
                &env_name,
                LmdbEnvironmentOptions::default(),
            )?;
            let meta_db = env.create_database(Some(METADATA_DB_NAME), None)?;
            let master_txn = env.create_txn()?;

            let mut commits = SourceStates::new();
            let mut input_schemas: HashMap<PortHandle, Schema> = HashMap::new();
            let mut output_schemas: HashMap<PortHandle, Schema> = HashMap::new();

            // Read schemas and checkpoints.
            {
                let txn = master_txn.read();
                let cur = txn.open_ro_cursor(meta_db)?;
                if !cur.first()? {
                    return Err(ExecutionError::InternalDatabaseError(
                        StorageError::InvalidRecord,
                    ));
                }

                loop {
                    let (key, value) = cur.read()?.ok_or(ExecutionError::InternalDatabaseError(
                        StorageError::InvalidRecord,
                    ))?;
                    match key[0] {
                        SOURCE_ID_IDENTIFIER => {
                            let (source, op_id) = deserialize_source_metadata(key, value);
                            commits.insert(source, op_id);
                        }
                        OUTPUT_SCHEMA_IDENTIFIER => {
                            let (handle, schema) = deserialize_schema(key, value)?;
                            output_schemas.insert(handle, schema);
                        }
                        INPUT_SCHEMA_IDENTIFIER => {
                            let (handle, schema) = deserialize_schema(key, value)?;
                            input_schemas.insert(handle, schema);
                        }
                        _ => {
                            return Err(ExecutionError::InternalDatabaseError(
                                StorageError::InvalidRecord,
                            ))
                        }
                    }
                    if !cur.next()? {
                        break;
                    }
                }
            }

            // Check if schema has changed.
            validate_schemas(
                &node.handle,
                SchemaType::Output,
                &dag_schemas.get_node_output_schemas(node_index),
                &output_schemas,
            )?;
            validate_schemas(
                &node.handle,
                SchemaType::Input,
                &dag_schemas.get_node_input_schemas(node_index),
                &input_schemas,
            )?;

            // Push metadata to vec.
            debug!(
                "[checkpoint] Node [{}] is at checkpoint {:?}",
                node.handle, commits
            );
            metadata.push(Some((
                Some(NodeStorage {
                    master_txn,
                    meta_db,
                }),
                commits,
            )));
        }

        let graph = dag_schemas.into_graph().map_owned(
            |node_index, node| {
                let (storage, commits) = metadata[node_index.index()]
                    .take()
                    .expect("We loaded all metadata");
                NodeType {
                    handle: node.handle,
                    commits,
                    storage,
                    kind: node.kind,
                }
            },
            |_, edge| edge,
        );
        Ok(Self { path, graph })
    }

    pub fn clear(&mut self) {
        for node in self.graph.node_weights_mut() {
            let env_name = node_environment_name(&node.handle);
            LmdbEnvironmentManager::remove(&self.path, &env_name);
            node.commits.clear();
        }
    }

    pub fn node_weight_mut(&mut self, node_index: NodeIndex) -> &mut NodeType<T> {
        &mut self.graph[node_index]
    }

    pub fn initialize_node_storage(
        &self,
        node_index: NodeIndex,
        storage_options: LmdbEnvironmentOptions,
    ) -> Result<NodeStorage, StorageError> {
        // Create the environment.
        let node = &self.graph[node_index];
        debug_assert!(node.storage.is_none());
        let node_handle = &node.handle;
        let env_name = node_environment_name(node_handle);
        debug_assert!(!LmdbEnvironmentManager::exists(&self.path, &env_name));
        let mut env = LmdbEnvironmentManager::create(&self.path, &env_name, storage_options)?;
        let meta_db = env.create_database(Some(METADATA_DB_NAME), Some(DatabaseFlags::empty()))?;
        let txn = env.create_txn()?;

        // Write schemas to the storage.
        write_schemas(
            &mut txn.write(),
            meta_db,
            OUTPUT_SCHEMA_IDENTIFIER,
            self.graph
                .edges_directed(node_index, Direction::Outgoing)
                .map(|edge| {
                    let edge = edge.weight();
                    (edge.output_port, &edge.schema)
                }),
        )?;
        write_schemas(
            &mut txn.write(),
            meta_db,
            INPUT_SCHEMA_IDENTIFIER,
            self.graph
                .edges_directed(node_index, Direction::Incoming)
                .map(|edge| {
                    let edge = edge.weight();
                    (edge.input_port, &edge.schema)
                }),
        )?;
        txn.write().commit_and_renew()?;

        // Return the storage.
        Ok(NodeStorage {
            master_txn: txn,
            meta_db,
        })
    }

    pub fn check_consistency(&self) -> bool {
        for node_index in Topo::new(&self.graph).iter(&self.graph) {
            let mut all_parent_commits = SourceStates::new();
            for edge in self.graph.edges_directed(node_index, Direction::Incoming) {
                let parent_commits = self.graph[edge.source()].commits.clone();
                all_parent_commits.extend(parent_commits.into_iter());
            }

            if all_parent_commits.is_empty() {
                // This is a source node.
                continue;
            }

            if all_parent_commits != self.graph[node_index].commits {
                debug!(
                    "Node [{}] has inconsistent commits: parents: {:?}, self: {:?}",
                    self.graph[node_index].handle,
                    all_parent_commits,
                    self.graph[node_index].commits
                );
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SchemaType {
    Input,
    Output,
}

fn node_environment_name(node: &NodeHandle) -> String {
    format!("{node}")
}

fn write_schemas<'a>(
    txn: &mut LmdbExclusiveTransaction,
    db: Database,
    identifier: u8,
    schemas: impl Iterator<Item = (PortHandle, &'a Schema)>,
) -> Result<(), StorageError> {
    for (handle, schema) in schemas {
        let mut key: Vec<u8> = vec![identifier];
        key.extend(handle.to_be_bytes());
        let value = bincode::serialize(schema).map_err(|e| SerializationError {
            typ: "Schema".to_string(),
            reason: Box::new(e),
        })?;
        txn.put(db, &key, &value)?;
    }
    Ok(())
}

fn deserialize_schema(key: &[u8], value: &[u8]) -> Result<(PortHandle, Schema), ExecutionError> {
    let handle: PortHandle = PortHandle::from_be_bytes(
        (&key[1..])
            .try_into()
            .map_err(|_e| ExecutionError::InvalidPortHandle(0))?,
    );
    let schema: Schema = bincode::deserialize(value).map_err(|e| DeserializationError {
        typ: "Schema".to_string(),
        reason: Box::new(e),
    })?;
    Ok((handle, schema))
}

pub fn write_source_metadata<'a>(
    txn: &mut LmdbExclusiveTransaction,
    db: Database,
    metadata: impl Iterator<Item = (&'a NodeHandle, OpIdentifier)>,
) -> Result<(), StorageError> {
    for (source, op_id) in metadata {
        let (key, value) = serialize_source_metadata(source, op_id);

        txn.put(db, &key, &value)?;
    }
    Ok(())
}

fn serialize_source_metadata(node_handle: &NodeHandle, op_id: OpIdentifier) -> (Vec<u8>, [u8; 16]) {
    let mut key: Vec<u8> = vec![SOURCE_ID_IDENTIFIER];
    key.extend(node_handle.to_bytes());

    let value = op_id.to_bytes();

    (key, value)
}

fn deserialize_source_metadata(key: &[u8], value: &[u8]) -> (NodeHandle, OpIdentifier) {
    debug_assert!(key[0] == SOURCE_ID_IDENTIFIER);
    let source = NodeHandle::from_bytes(&key[1..]);

    let op_id = OpIdentifier::from_bytes(value.try_into().unwrap());
    (source, op_id)
}

fn validate_schemas(
    node_handle: &NodeHandle,
    typ: SchemaType,
    current: &HashMap<PortHandle, Schema>,
    existing: &HashMap<PortHandle, Schema>,
) -> Result<(), ExecutionError> {
    if existing.len() != current.len() {
        return Err(ExecutionError::IncompatibleSchemas {
            node: node_handle.clone(),
            typ,
            source: IncompatibleSchemas::LengthMismatch {
                current: current.len(),
                existing: existing.len(),
            },
        });
    }
    for (port, current_schema) in current {
        let existing_schema =
            existing
                .get(port)
                .ok_or_else(|| ExecutionError::IncompatibleSchemas {
                    node: node_handle.clone(),
                    typ,
                    source: IncompatibleSchemas::NotFound(*port),
                })?;

        if current_schema != existing_schema {
            current_schema.print().printstd();
            existing_schema.print().printstd();
            return Err(ExecutionError::IncompatibleSchemas {
                node: node_handle.clone(),
                typ,
                source: IncompatibleSchemas::SchemaMismatch(*port),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_metadata_serialization() {
        fn check(node_handle: NodeHandle, op_id: OpIdentifier) {
            let (key, value) = serialize_source_metadata(&node_handle, op_id);
            let (node_handle2, op_id2) = deserialize_source_metadata(&key, &value);
            assert_eq!(node_handle2, node_handle);
            assert_eq!(op_id2, op_id);
        }

        check(
            NodeHandle::new(None, "node".to_string()),
            OpIdentifier::new(0, 0),
        );
    }

    #[test]
    #[should_panic]
    fn source_metadata_deserialization_panics_on_empty_key() {
        deserialize_source_metadata(&[], &[]);
    }

    #[test]
    #[should_panic]
    fn source_metadata_deserialization_panics_on_invalid_key() {
        let (mut key, _) = serialize_source_metadata(
            &NodeHandle::new(None, "node".to_string()),
            OpIdentifier::default(),
        );
        key[0] = 1;
        deserialize_source_metadata(&key, &[]);
    }

    #[test]
    #[should_panic]
    fn source_metadata_deserialization_panics_on_empty_value() {
        let (key, _) = serialize_source_metadata(
            &NodeHandle::new(None, "node".to_string()),
            OpIdentifier::default(),
        );
        deserialize_source_metadata(&key, &[]);
    }
}
