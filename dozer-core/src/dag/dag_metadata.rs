use crate::dag::dag::{Dag, Edge, NodeType};
use crate::dag::dag_schemas::NodeSchemas;
use crate::dag::errors::ExecutionError;
use crate::dag::errors::ExecutionError::{
    InvalidCheckpointState, InvalidNodeHandle, MetadataAlreadyExists,
};
use crate::dag::node::{NodeHandle, PortHandle};
use crate::storage::errors::StorageError;
use crate::storage::errors::StorageError::{DeserializationError, SerializationError};
use crate::storage::lmdb_storage::LmdbEnvironmentManager;
use dozer_types::types::Schema;
use std::collections::{HashMap, HashSet};

use std::path::Path;

pub(crate) const METADATA_DB_NAME: &str = "__META__";
pub(crate) const SOURCE_ID_IDENTIFIER: u8 = 0_u8;
pub(crate) const OUTPUT_SCHEMA_IDENTIFIER: u8 = 1_u8;
pub(crate) const INPUT_SCHEMA_IDENTIFIER: u8 = 2_u8;

pub(crate) enum Consistency {
    FullyConsistent((u64, u64)),
    PartiallyConsistent(HashMap<(u64, u64), Vec<NodeHandle>>),
}

struct DependencyTreeNode {
    pub handle: NodeHandle,
    pub children: Vec<DependencyTreeNode>,
}

impl DependencyTreeNode {
    pub fn new(handle: NodeHandle) -> Self {
        Self {
            handle,
            children: Vec::new(),
        }
    }
}

pub(crate) struct DagMetadata {
    pub commits: HashMap<NodeHandle, (u64, u64)>,
    pub input_schemas: HashMap<PortHandle, Schema>,
    pub output_schemas: HashMap<PortHandle, Schema>,
}

pub(crate) struct DagMetadataManager<'a> {
    dag: &'a Dag,
    path: &'a Path,
    metadata: HashMap<NodeHandle, DagMetadata>,
    deps_trees: HashMap<NodeHandle, DependencyTreeNode>,
}

impl<'a> DagMetadataManager<'a> {
    pub fn new(dag: &'a Dag, path: &'a Path) -> Result<DagMetadataManager<'a>, ExecutionError> {
        let metadata = DagMetadataManager::get_checkpoint_metadata(path, dag)?;
        let mut deps_trees: HashMap<NodeHandle, DependencyTreeNode> = HashMap::new();

        for src in dag
            .nodes
            .iter()
            .filter(|e| matches!(e.1, NodeType::Source(_)))
            .map(|e| e.0)
        {
            let mut root = DependencyTreeNode::new(src.clone());
            Self::get_source_dependency_tree(&mut root, dag);
            deps_trees.insert(src.clone(), root);
        }

        Ok(Self {
            path,
            dag,
            metadata,
            deps_trees,
        })
    }

    fn get_node_checkpoint_metadata(
        path: &Path,
        name: &NodeHandle,
    ) -> Result<DagMetadata, ExecutionError> {
        if !LmdbEnvironmentManager::exists(path, format!("{}", name).as_str()) {
            return Err(InvalidCheckpointState(name.clone()));
        }

        let mut env = LmdbEnvironmentManager::create(path, format!("{}", name).as_str())?;
        let db = env.open_database(METADATA_DB_NAME, false, None)?;
        let txn = env.create_txn()?;

        let cur = txn.open_cursor(&db)?;
        if !cur.first()? {
            return Err(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ));
        }

        let mut map = HashMap::<NodeHandle, (u64, u64)>::new();
        let mut input_schemas: HashMap<PortHandle, Schema> = HashMap::new();
        let mut output_schemas: HashMap<PortHandle, Schema> = HashMap::new();

        loop {
            let value = cur.read()?.ok_or(ExecutionError::InternalDatabaseError(
                StorageError::InvalidRecord,
            ))?;
            match value.0[0] {
                SOURCE_ID_IDENTIFIER => {
                    let handle: NodeHandle = NodeHandle::from_bytes(&value.0[1..]);
                    let txid: u64 = u64::from_be_bytes(value.1[0..8].try_into().unwrap());
                    let seq_in_tx: u64 = u64::from_be_bytes(value.1[8..16].try_into().unwrap());
                    map.insert(handle, (txid, seq_in_tx));
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

        Ok(DagMetadata {
            commits: map,
            input_schemas,
            output_schemas,
        })
    }

    fn get_checkpoint_metadata(
        path: &Path,
        dag: &Dag,
    ) -> Result<HashMap<NodeHandle, DagMetadata>, ExecutionError> {
        let mut all = HashMap::<NodeHandle, DagMetadata>::new();
        for node in &dag.nodes {
            match DagMetadataManager::get_node_checkpoint_metadata(path, node.0) {
                Ok(r) => {
                    all.insert(node.0.clone(), r);
                }
                Err(_e) => LmdbEnvironmentManager::remove(path, format!("{}", node.0).as_str()),
            }
        }
        Ok(all)
    }

    fn get_state_seq_for_node(
        &self,
        src: &NodeHandle,
        curr: &NodeHandle,
    ) -> Result<(u64, u64), ExecutionError> {
        let node_meta = self
            .metadata
            .get(curr)
            .ok_or_else(|| ExecutionError::InvalidCheckpointState(curr.clone()))?;
        node_meta
            .commits
            .get(src)
            .copied()
            .ok_or_else(|| ExecutionError::InvalidCheckpointState(curr.clone()))
    }

    fn get_source_dependency_tree(curr: &mut DependencyTreeNode, dag: &Dag) {
        let children: Vec<&Edge> = dag
            .edges
            .iter()
            .filter(|e| e.from.node == curr.handle)
            .collect();

        for child in children {
            let mut new_node = DependencyTreeNode::new(child.to.node.clone());
            Self::get_source_dependency_tree(&mut new_node, dag);
            curr.children.push(new_node);
        }
    }

    fn get_sources_for_namespace(&self, ns: u16) -> HashSet<NodeHandle> {
        let mut handles = HashSet::<NodeHandle>::new();
        for (src_handle, src_node) in self.deps_trees.iter() {
            if src_node.children.iter().any(|e| match e.handle.ns {
                Some(node_ns) => ns == node_ns,
                _ => false,
            }) {
                handles.insert(src_handle.clone());
            }
        }
        handles
    }

    fn get_dependency_tree_consistency_rec(
        &self,
        source_handle: &NodeHandle,
        tree_node: &DependencyTreeNode,
        res: &mut HashMap<(u64, u64), Vec<NodeHandle>>,
    ) {
        let seq = match self.metadata.get(&tree_node.handle) {
            Some(v) => *v.commits.get(source_handle).unwrap_or(&(0, 0)),
            None => (0, 0),
        };
        res.entry(seq).or_insert_with(Vec::new);
        res.get_mut(&seq).unwrap().push(tree_node.handle.clone());

        for child in &tree_node.children {
            self.get_dependency_tree_consistency_rec(source_handle, child, res);
        }
    }

    pub(crate) fn get_checkpoint_consistency(&self) -> HashMap<NodeHandle, Consistency> {
        let mut r: HashMap<NodeHandle, Consistency> = HashMap::new();
        for e in &self.deps_trees {
            let mut res: HashMap<(u64, u64), Vec<NodeHandle>> = HashMap::new();
            self.get_dependency_tree_consistency_rec(&e.1.handle, e.1, &mut res);
            match res.len() {
                1 => r.insert(
                    e.0.clone(),
                    Consistency::FullyConsistent(*res.iter().next().unwrap().0),
                ),
                _ => r.insert(e.0.clone(), Consistency::PartiallyConsistent(res)),
            };
        }
        r
    }

    pub(crate) fn delete_metadata(&self) {
        for node in &self.dag.nodes {
            LmdbEnvironmentManager::remove(self.path, format!("{}", node.0).as_str());
        }
    }

    pub(crate) fn get_metadata(&self) -> Result<HashMap<NodeHandle, DagMetadata>, ExecutionError> {
        let mut all_meta = HashMap::<NodeHandle, DagMetadata>::new();
        for node in &self.dag.nodes {
            let metadata = Self::get_node_checkpoint_metadata(self.path, node.0)?;
            all_meta.insert(node.0.clone(), metadata);
        }
        Ok(all_meta)
    }

    pub(crate) fn init_metadata(
        &self,
        schemas: &HashMap<NodeHandle, NodeSchemas>,
    ) -> Result<(), ExecutionError> {
        for node in &self.dag.nodes {
            let curr_node_schema = schemas
                .get(node.0)
                .ok_or_else(|| InvalidNodeHandle(node.0.clone()))?;

            if LmdbEnvironmentManager::exists(self.path, format!("{}", node.0).as_str()) {
                return Err(MetadataAlreadyExists(node.0.clone()));
            }

            let mut env =
                LmdbEnvironmentManager::create(self.path, format!("{}", node.0).as_str())?;
            let db = env.open_database(METADATA_DB_NAME, false, None)?;
            let mut txn = env.create_txn()?;

            for (handle, schema) in curr_node_schema.output_schemas.iter() {
                let mut key: Vec<u8> = vec![OUTPUT_SCHEMA_IDENTIFIER];
                key.extend(handle.to_be_bytes());
                let value = bincode::serialize(schema).map_err(|e| SerializationError {
                    typ: "Schema".to_string(),
                    reason: Box::new(e),
                })?;
                txn.put(&db, &key, &value)?;
            }

            for (handle, schema) in curr_node_schema.input_schemas.iter() {
                let mut key: Vec<u8> = vec![INPUT_SCHEMA_IDENTIFIER];
                key.extend(handle.to_be_bytes());
                let value = bincode::serialize(schema).map_err(|e| SerializationError {
                    typ: "Schema".to_string(),
                    reason: Box::new(e),
                })?;
                txn.put(&db, &key, &value)?;
            }

            for (source, _factory) in &self.dag.get_sources() {
                let mut key: Vec<u8> = vec![SOURCE_ID_IDENTIFIER];
                key.extend(source.to_bytes());

                let mut value: Vec<u8> = Vec::with_capacity(16);
                value.extend(0_u64.to_be_bytes());
                value.extend(0_u64.to_be_bytes());

                txn.put(&db, &key, &value)?;
            }

            txn.commit_and_renew()?;
        }
        Ok(())
    }
}
