use crate::dag::dag_schemas::NodeSchemas;
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{InvalidNodeHandle, MetadataAlreadyExists};
use crate::dag::node::{NodeHandle, PortHandle};
use crate::dag::Dag;
use crate::storage::common::Seek;
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::{DeserializationError, SerializationError};
use crate::storage::lmdb_storage::{
    LmdbEnvironmentManager, LmdbExclusiveTransaction, SharedTransaction,
};
use dozer_types::bincode;
use dozer_types::types::Schema;
use lmdb::Database;
use std::collections::HashMap;

use std::iter::once;
use std::path::Path;

use super::epoch::{OpIdentifier, SourceStates};
use super::hash_map_to_vec::insert_vec_element;

pub(crate) const METADATA_DB_NAME: &str = "__META__";
const SOURCE_ID_IDENTIFIER: u8 = 0_u8;
pub(crate) const OUTPUT_SCHEMA_IDENTIFIER: u8 = 1_u8;
pub(crate) const INPUT_SCHEMA_IDENTIFIER: u8 = 2_u8;

pub(crate) enum Consistency {
    FullyConsistent(Option<OpIdentifier>),
    PartiallyConsistent(HashMap<Option<OpIdentifier>, Vec<NodeHandle>>),
}

pub(crate) struct DagMetadata {
    pub commits: SourceStates,
    pub input_schemas: HashMap<PortHandle, Schema>,
    pub output_schemas: HashMap<PortHandle, Schema>,
}

pub(crate) struct DagMetadataManager<'a, T: Clone> {
    dag: &'a Dag<T>,
    path: &'a Path,
    metadata: HashMap<NodeHandle, DagMetadata>,
}

impl<'a, T: Clone + 'a> DagMetadataManager<'a, T> {
    pub fn new(
        dag: &'a Dag<T>,
        path: &'a Path,
    ) -> Result<DagMetadataManager<'a, T>, ExecutionError> {
        let metadata = DagMetadataManager::get_checkpoint_metadata(path, dag)?;

        Ok(Self {
            path,
            dag,
            metadata,
        })
    }

    fn get_node_checkpoint_metadata(
        path: &Path,
        name: &NodeHandle,
    ) -> Result<Option<DagMetadata>, ExecutionError> {
        let env_name = metadata_environment_name(name);
        if !LmdbEnvironmentManager::exists(path, &env_name) {
            return Ok(None);
        }

        let mut env = LmdbEnvironmentManager::create(path, &env_name)?;
        let db = env.open_database(METADATA_DB_NAME, false)?;
        let txn = env.create_txn()?;
        let txn = SharedTransaction::try_unwrap(txn)
            .expect("We just created this `SharedTransaction`. It's not shared.");

        let cur = txn.open_ro_cursor(db)?;
        if !cur.first()? {
            return Err(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ));
        }

        let mut commits = SourceStates::default();
        let mut input_schemas: HashMap<PortHandle, Schema> = HashMap::new();
        let mut output_schemas: HashMap<PortHandle, Schema> = HashMap::new();

        loop {
            let value = cur.read()?.ok_or(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ))?;
            match value.0[0] {
                SOURCE_ID_IDENTIFIER => {
                    commits.extend(once(deserialize_source_metadata(value.0, value.1)))
                }
                OUTPUT_SCHEMA_IDENTIFIER => {
                    let handle: PortHandle = PortHandle::from_be_bytes(
                        (&value.0[1..])
                            .try_into()
                            .map_err(|_e| ExecutionError::InvalidPortHandle(0))?,
                    );
                    let schema: Schema =
                        bincode::deserialize(value.1).map_err(|e| DeserializationError {
                            typ: "Schema".to_string(),
                            reason: Box::new(e),
                        })?;
                    output_schemas.insert(handle, schema);
                }
                INPUT_SCHEMA_IDENTIFIER => {
                    let handle: PortHandle = PortHandle::from_be_bytes(
                        (&value.0[1..])
                            .try_into()
                            .map_err(|_e| ExecutionError::InvalidPortHandle(0))?,
                    );
                    let schema: Schema =
                        bincode::deserialize(value.1).map_err(|e| DeserializationError {
                            typ: "Schema".to_string(),
                            reason: Box::new(e),
                        })?;
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

        Ok(Some(DagMetadata {
            commits,
            input_schemas,
            output_schemas,
        }))
    }

    fn get_checkpoint_metadata(
        path: &Path,
        dag: &Dag<T>,
    ) -> Result<HashMap<NodeHandle, DagMetadata>, ExecutionError> {
        let mut all = HashMap::<NodeHandle, DagMetadata>::new();
        for node in dag.node_handles() {
            if let Some(metadata) = Self::get_node_checkpoint_metadata(path, node)? {
                all.insert(node.clone(), metadata);
            }
        }
        Ok(all)
    }

    fn get_dependency_tree_consistency(
        &self,
        root_node: &NodeHandle,
    ) -> HashMap<Option<OpIdentifier>, Vec<NodeHandle>> {
        let mut result = HashMap::new();

        for node_handle in self.dag.bfs(root_node) {
            let seq = self
                .metadata
                .get(node_handle)
                .and_then(|dag_meta_data| dag_meta_data.commits.get(root_node).copied());

            insert_vec_element(&mut result, seq, node_handle.clone());
        }

        result
    }

    pub(crate) fn get_checkpoint_consistency(&self) -> HashMap<NodeHandle, Consistency> {
        let mut r: HashMap<NodeHandle, Consistency> = HashMap::new();
        for node_handle in self.dag.node_handles() {
            let consistency = self.get_dependency_tree_consistency(node_handle);
            debug_assert!(!consistency.is_empty());
            if consistency.len() == 1 {
                r.insert(
                    node_handle.clone(),
                    Consistency::FullyConsistent(*consistency.iter().next().unwrap().0),
                );
            } else {
                r.insert(
                    node_handle.clone(),
                    Consistency::PartiallyConsistent(consistency),
                );
            }
        }
        r
    }

    pub(crate) fn delete_metadata(&self) {
        for node in self.dag.node_handles() {
            LmdbEnvironmentManager::remove(self.path, &metadata_environment_name(node));
        }
    }

    pub(crate) fn get_metadata(&self) -> Result<HashMap<NodeHandle, DagMetadata>, ExecutionError> {
        Self::get_checkpoint_metadata(self.path, self.dag)
    }

    pub(crate) fn init_metadata(
        &self,
        schemas: &HashMap<NodeHandle, NodeSchemas<T>>,
    ) -> Result<(), ExecutionError> {
        for node in self.dag.node_handles() {
            let curr_node_schema = schemas
                .get(node)
                .ok_or_else(|| InvalidNodeHandle(node.clone()))?;

            let env_name = metadata_environment_name(node);
            if LmdbEnvironmentManager::exists(self.path, &env_name) {
                return Err(MetadataAlreadyExists(node.clone()));
            }

            let mut env = LmdbEnvironmentManager::create(self.path, &env_name)?;
            let db = env.open_database(METADATA_DB_NAME, false)?;
            let txn = env.create_txn()?;
            let mut txn = SharedTransaction::try_unwrap(txn)
                .expect("We just created this `SharedTransaction`. It's not shared.");

            for (handle, (schema, _ctx)) in curr_node_schema.output_schemas.iter() {
                let mut key: Vec<u8> = vec![OUTPUT_SCHEMA_IDENTIFIER];
                key.extend(handle.to_be_bytes());
                let value = bincode::serialize(schema).map_err(|e| SerializationError {
                    typ: "Schema".to_string(),
                    reason: Box::new(e),
                })?;
                txn.put(db, &key, &value)?;
            }

            for (handle, (schema, _ctx)) in curr_node_schema.input_schemas.iter() {
                let mut key: Vec<u8> = vec![INPUT_SCHEMA_IDENTIFIER];
                key.extend(handle.to_be_bytes());
                let value = bincode::serialize(schema).map_err(|e| SerializationError {
                    typ: "Schema".to_string(),
                    reason: Box::new(e),
                })?;
                txn.put(db, &key, &value)?;
            }

            txn.commit_and_renew()?;
        }
        Ok(())
    }
}

fn metadata_environment_name(node_handle: &NodeHandle) -> String {
    format!("{node_handle}")
}

pub fn write_source_metadata<'a>(
    txn: &mut LmdbExclusiveTransaction,
    db: Database,
    metadata: &'a mut impl Iterator<Item = (&'a NodeHandle, OpIdentifier)>,
) -> Result<(), StorageError> {
    for (source, op_id) in metadata {
        let (key, value) = serialize_source_metadata(source, op_id);

        txn.put(db, &key, &value)?;
    }
    Ok(())
}

fn serialize_source_metadata(node_handle: &NodeHandle, op_id: OpIdentifier) -> (Vec<u8>, Vec<u8>) {
    let mut key: Vec<u8> = vec![SOURCE_ID_IDENTIFIER];
    key.extend(node_handle.to_bytes());

    let mut value: Vec<u8> = Vec::with_capacity(16);
    value.extend(op_id.txid.to_be_bytes());
    value.extend(op_id.seq_in_tx.to_be_bytes());

    (key, value)
}

fn deserialize_source_metadata(key: &[u8], value: &[u8]) -> (NodeHandle, OpIdentifier) {
    debug_assert!(key[0] == SOURCE_ID_IDENTIFIER);
    let source = NodeHandle::from_bytes(&key[1..]);

    let txid = u64::from_be_bytes(value[0..8].try_into().unwrap());
    let seq_in_tx = u64::from_be_bytes(value[8..16].try_into().unwrap());
    (source, OpIdentifier { txid, seq_in_tx })
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
