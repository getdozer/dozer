use std::collections::HashMap;
use std::ffi::{CStr, CString, NulError};

use dozer_core::daggy::petgraph::Direction;
use dozer_core::daggy::{self, EdgeIndex, NodeIndex};
use dozer_core::petgraph::visit::{
    EdgeRef, IntoEdgesDirected, IntoNeighborsDirected, IntoNodeReferences,
};
use dozer_types::indexmap::IndexMap;

use dozer_types::models::sink::{AerospikeSet, AerospikeSinkTable};
use dozer_types::thiserror;
use dozer_types::types::{Field, Record, Schema, TableOperation};
use itertools::{Either, Itertools};
use smallvec::SmallVec;

use crate::aerospike::{
    parse_record, parse_record_many, BinNames, Client, ReadBatch, ReadBatchResults, WriteBatch,
};
use crate::AerospikeSinkError;

const MANY_LIST_BIN: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked("data\0".as_bytes()) };

#[derive(Debug, Clone)]
struct CachedRecord {
    dirty: bool,
    version: usize,
    record: Option<Vec<Field>>,
}

#[derive(Debug, Clone, Default)]
struct OneToOneBatch(IndexMap<Vec<Field>, SmallVec<[CachedRecord; 2]>>);

#[derive(Debug, Clone)]
enum ManyOp {
    Add(Vec<Field>),
    Remove(Vec<Field>),
}

#[derive(Debug, Clone)]
struct ManyRecord {
    version: usize,
    ops: Vec<ManyOp>,
}

#[derive(Debug, Clone, Default)]
struct OneToManyEntry {
    base: Option<Vec<Vec<Field>>>,
    ops: Vec<ManyRecord>,
}

#[derive(Debug, Clone, Default)]
struct OneToManyBatch(IndexMap<Vec<Field>, OneToManyEntry>);

impl OneToManyBatch {
    fn insert_point(
        &mut self,
        key: Vec<Field>,
        version: usize,
    ) -> (&mut OneToManyEntry, usize, usize) {
        let entry = self.0.entry(key);
        let idx = entry.index();
        let entry = entry.or_default();
        let insert_point = entry
            .ops
            .iter()
            .position(|rec| rec.version >= version)
            .unwrap_or(entry.ops.len());
        (entry, idx, insert_point)
    }

    fn insert_local(&mut self, key: Vec<Field>, value: Vec<Field>, version: usize) -> usize {
        let (entry, idx, insert_point) = self.insert_point(key, version);
        match entry.ops.get_mut(insert_point) {
            Some(entry) if entry.version == version => {
                entry.ops.push(ManyOp::Add(value));
            }
            _ => {
                entry.ops.insert(
                    insert_point,
                    ManyRecord {
                        version,
                        ops: vec![ManyOp::Add(value)],
                    },
                );
            }
        }
        idx
    }

    fn remove_local(&mut self, key: Vec<Field>, old_value: &[Field], version: usize) -> usize {
        let (entry, idx, insert_point) = self.insert_point(key, version);
        match entry.ops.get_mut(insert_point) {
            Some(entry) if entry.version == version => {
                if let Some(added) = entry
                    .ops
                    .iter()
                    .position(|entry| matches!(entry, ManyOp::Add(value) if value == old_value))
                {
                    let _ = entry.ops.swap_remove(added);
                } else {
                    entry.ops.push(ManyOp::Remove(old_value.to_vec()));
                }
            }
            _ => entry.ops.insert(
                insert_point,
                ManyRecord {
                    version,
                    ops: vec![ManyOp::Remove(old_value.to_vec())],
                },
            ),
        };
        idx
    }

    fn replace_local(
        &mut self,
        key: Vec<Field>,
        old_value: Vec<Field>,
        new_value: Vec<Field>,
        version: usize,
    ) -> usize {
        let (entry, idx, insert_point) = self.insert_point(key, version);
        match entry.ops.get_mut(insert_point) {
            Some(entry) if entry.version == version => {
                if let Some(added) = entry
                    .ops
                    .iter_mut()
                    .find(|entry| matches!(entry, ManyOp::Add(value) if value == &old_value))
                {
                    *added = ManyOp::Add(new_value);
                } else {
                    entry.ops.push(ManyOp::Remove(old_value));
                    entry.ops.push(ManyOp::Add(new_value));
                }
            }
            _ => entry.ops.insert(
                insert_point,
                ManyRecord {
                    version,
                    ops: vec![ManyOp::Remove(old_value), ManyOp::Add(new_value)],
                },
            ),
        };
        idx
    }

    fn insert_remote(&mut self, index: usize, value: Vec<Vec<Field>>) {
        let (_, record) = self.0.get_index_mut(index).unwrap();
        record.base = Some(value);
    }

    fn get(&self, key: &[Field], version: usize) -> Option<impl Iterator<Item = Vec<Field>>> {
        let entry = self.0.get(key)?;

        Self::get_inner(entry, version)
    }
    fn get_index(&self, index: usize, version: usize) -> Option<impl Iterator<Item = Vec<Field>>> {
        let (_, entry) = self.0.get_index(index)?;

        Self::get_inner(entry, version)
    }

    fn get_inner(entry: &OneToManyEntry, version: usize) -> Option<std::vec::IntoIter<Vec<Field>>> {
        let mut recs = entry.base.clone()?;
        for version in entry.ops.iter().take_while(|ops| ops.version <= version) {
            for op in &version.ops {
                match op {
                    ManyOp::Add(rec) => recs.push(rec.clone()),
                    ManyOp::Remove(to_remove) => {
                        if let Some(to_remove) = recs.iter().position(|rec| rec == to_remove) {
                            recs.swap_remove(to_remove);
                        }
                    }
                }
            }
        }
        if recs.is_empty() {
            None
        } else {
            Some(recs.into_iter())
        }
    }

    fn write(
        &mut self,
        record_batch: &mut WriteBatch,
        schema: &AerospikeSchema,
    ) -> Result<(), AerospikeSinkError> {
        for (k, v) in self.0.drain(..) {
            // We should always have a base, otherwise we can't do idempotent writes
            let mut record = v.base.unwrap();
            // Apply ops
            for version in v.ops {
                for op in version.ops {
                    match op {
                        ManyOp::Add(rec) => {
                            record.push(rec);
                        }
                        ManyOp::Remove(rec) => {
                            if let Some(pos) = record.iter().position(|r| r == &rec) {
                                record.swap_remove(pos);
                            }
                        }
                    }
                }
            }
            record_batch.add_write_list(
                &schema.namespace,
                &schema.set,
                MANY_LIST_BIN,
                &k,
                schema.bins.names(),
                &record,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum CachedBatch {
    One(OneToOneBatch),
    Many(OneToManyBatch),
}
struct DirtyRecord<'a> {
    idx: usize,
    key: &'a [Field],
    version: usize,
}

impl CachedBatch {
    fn iter_dirty(&self) -> impl Iterator<Item = DirtyRecord> {
        match self {
            Self::One(batch) => batch
                .0
                .iter()
                .enumerate()
                .filter_map(|(i, (k, v))| Some((i, k, v.last()?)))
                .filter(|(_, _, v)| v.dirty)
                .map(|(i, k, v)| DirtyRecord {
                    idx: i,
                    key: k,
                    version: v.version,
                }),
            Self::Many(_) => unimplemented!(),
        }
    }

    fn remove_local(&mut self, key: Vec<Field>, old_value: &[Field], version: usize) -> usize {
        match self {
            Self::One(batch) => batch.insert_local(key, None, version),
            Self::Many(batch) => batch.remove_local(key, old_value, version),
        }
    }

    fn insert_local(&mut self, key: Vec<Field>, value: Vec<Field>, version: usize) -> usize {
        match self {
            Self::One(batch) => batch.insert_local(key, Some(value), version),
            Self::Many(batch) => batch.insert_local(key, value, version),
        }
    }

    fn replace_local(
        &mut self,
        key: Vec<Field>,
        old_value: Vec<Field>,
        new_value: Vec<Field>,
        version: usize,
    ) -> usize {
        match self {
            Self::One(batch) => batch.insert_impl(key, Some(new_value), version, true, true),
            Self::Many(batch) => batch.replace_local(key, old_value, new_value, version),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::One(batch) => batch.clear(),
            Self::Many(batch) => batch.0.clear(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::One(batch) => batch.len(),
            Self::Many(batch) => batch.0.len(),
        }
    }

    fn write(
        &mut self,
        record_batch: &mut WriteBatch,
        schema: &AerospikeSchema,
    ) -> Result<(), AerospikeSinkError> {
        match self {
            Self::One(batch) => batch.write(record_batch, schema),
            Self::Many(batch) => batch.write(record_batch, schema),
        }
    }

    fn get<'a>(
        &'a self,
        key: &[Field],
        version: usize,
    ) -> Option<impl Iterator<Item = Vec<Field>> + 'a> {
        match self {
            Self::One(batch) => {
                let record = batch.get(key, version)?.record.clone()?;
                Some(Either::Left(std::iter::once(record)))
            }
            Self::Many(batch) => Some(Either::Right(batch.get(key, version)?)),
        }
    }

    fn get_index(
        &self,
        index: usize,
        version: usize,
    ) -> Option<impl Iterator<Item = Vec<Field>> + '_> {
        match self {
            Self::One(batch) => {
                let record = batch.get_index(index, version)?.record.clone()?;
                Some(Either::Left(std::iter::once(record)))
            }
            Self::Many(batch) => Some(Either::Right(batch.get_index(index, version)?)),
        }
    }

    fn should_update_at(&mut self, key: Vec<Field>, version: usize) -> (bool, usize) {
        match self {
            Self::One(batch) => {
                let (index, exists) = batch.index_or_default(key, version);
                (!exists, index)
            }
            // For a many batch, we always need the base from the remote
            Self::Many(batch) => {
                let entry = batch.0.entry(key);
                let idx = entry.index();
                (entry.or_default().base.is_none(), idx)
            }
        }
    }
}

impl OneToOneBatch {
    fn insert_local(
        &mut self,
        key: Vec<Field>,
        value: Option<Vec<Field>>,
        version: usize,
    ) -> usize {
        self.insert_impl(key, value, version, true, true)
    }

    fn insert_impl(
        &mut self,
        key: Vec<Field>,
        value: Option<Vec<Field>>,
        version: usize,
        replace: bool,
        dirty: bool,
    ) -> usize {
        let entry = self.0.entry(key);
        let idx = entry.index();
        let versions = entry.or_default();
        let record = CachedRecord {
            dirty,
            version,
            record: value,
        };
        // This is basically partition_by, but that does a binary search, while
        // a linear search should in general be a better bet here
        let insert_point = versions
            .iter()
            .position(|cur| cur.version >= version)
            .unwrap_or(versions.len());
        // If the version already exists, replace it
        if versions
            .get(insert_point)
            .is_some_and(|rec| rec.version == version)
        {
            if replace {
                versions[insert_point] = record;
            }
        } else {
            versions.insert(insert_point, record);
        }
        idx
    }

    fn insert_remote(&mut self, index: usize, value: Option<Vec<Field>>) {
        let (_, versions) = self.0.get_index_mut(index).unwrap();
        versions.insert(
            0,
            CachedRecord {
                dirty: false,
                version: 0,
                record: value,
            },
        );
    }

    fn get<'a>(&'a self, key: &[Field], version: usize) -> Option<&'a CachedRecord> {
        let versions = self.0.get(key)?;
        // Find the last version thats <= version
        versions.iter().take_while(|v| v.version <= version).last()
    }

    fn get_index(&self, index: usize, version: usize) -> Option<&CachedRecord> {
        let (_, versions) = self.0.get_index(index)?;
        // Find the last version thats <= version
        versions.iter().take_while(|v| v.version <= version).last()
    }

    /// Returns the index at which the entry for the given key exists,
    /// or was created and whether it existed
    fn index_or_default(&mut self, key: Vec<Field>, version: usize) -> (usize, bool) {
        let entry = self.0.entry(key);
        let idx = entry.index();
        let versions = entry.or_default();
        (idx, versions.first().is_some_and(|v| v.version <= version))
    }

    fn clear(&mut self) {
        self.0.clear()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn write(
        &mut self,
        batch: &mut WriteBatch,
        schema: &AerospikeSchema,
    ) -> Result<(), AerospikeSinkError> {
        for (key, dirty_record) in self.0.drain(..).filter_map(|(key, mut rec)| {
            let last_version = rec.pop()?;
            last_version.dirty.then_some((key, last_version.record))
        }) {
            if let Some(dirty_record) = dirty_record {
                batch.add_write(
                    &schema.namespace,
                    &schema.set,
                    schema.bins.names(),
                    &key,
                    &dirty_record,
                )?;
            } else {
                batch.add_remove(&schema.namespace, &schema.set, &key)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct AerospikeSchema {
    namespace: CString,
    set: CString,
    bins: BinNames,
}

#[derive(Debug, Clone)]
struct Node {
    schema: Schema,
    batch: CachedBatch,
    as_schema: AerospikeSchema,
    denormalize_to: Option<(CString, CString, Vec<String>)>,
}

#[derive(Debug, Clone, PartialEq, Hash, Eq)]
struct LookupSource {
    index: usize,
    version: usize,
}

#[derive(Debug, Clone)]
struct Edge {
    bins: BinNames,
    key_fields: Vec<usize>,
    field_indices: Vec<usize>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("Duplicate sink set definition: {namespace}.{set}")]
    DuplicateSinkTable { namespace: String, set: String },
    #[error("Set referenced in denormalization not found: {namespace}.{set}")]
    SetNotFound { namespace: String, set: String },
    #[error("Adding denormalizing lookup on set {namespace}.{set} from set {from_namespace}.{from_set} would create a cycle")]
    Cycle {
        namespace: String,
        set: String,
        from_namespace: String,
        from_set: String,
    },
    #[error("Field not found")]
    FieldNotFound(String),
    #[error("Invalid name")]
    InvalidName(#[from] NulError),
    #[error("Non-nullible lookup value not found")]
    NotNullNotFound,
    #[error("The primary key for lookup set \"{lookup_namespace}\".\"{lookup_set}\" does not match the denormalization key specified by the denormalizing set \"{denorm_namespace}\".\"{denorm_set}\"")]
    MismatchedKeys {
        lookup_namespace: String,
        lookup_set: String,
        denorm_namespace: String,
        denorm_set: String,
    },
}

#[derive(Debug)]
pub(crate) struct DenormalizationState {
    dag: DenormDag,
    current_transaction: Option<u64>,
    base_tables: Vec<(NodeIndex, Vec<CString>, Vec<usize>)>,
    transaction_counter: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) struct DenormalizedTable {
    pub(crate) bin_names: Vec<CString>,
    pub(crate) namespace: CString,
    pub(crate) set: CString,
    pub(crate) records: Vec<Vec<Field>>,
    pub(crate) pk: Vec<usize>,
}
type DenormDag = daggy::Dag<Node, Edge>;

fn bin_names_recursive(dag: &DenormDag, nid: NodeIndex, bins: &mut Vec<CString>) {
    let mut neighbors = dag.neighbors_directed(nid, Direction::Outgoing).detach();
    while let Some((edge, node)) = neighbors.next(dag.graph()) {
        let edge = dag.edge_weight(edge).unwrap();
        bins.extend_from_slice(edge.bins.names());
        bin_names_recursive(dag, node, bins);
    }
}

impl DenormalizationState {
    fn node(&self, index: NodeIndex) -> &Node {
        self.dag.node_weight(index).unwrap()
    }

    fn edge(&self, index: EdgeIndex) -> &Edge {
        self.dag.edge_weight(index).unwrap()
    }
}

impl DenormalizationState {
    pub(crate) fn new(tables: &[(AerospikeSinkTable, Schema)]) -> Result<Self, Error> {
        assert!(!tables.is_empty());
        let dag = Self::build_dag(tables)?;
        let base_tables: Vec<_> = dag
            .node_references()
            // Filter out non-base-tables
            .filter_map(|(i, node)| node.denormalize_to.as_ref().map(|(_, _, pk)| (i, pk)))
            // Find all added bin names using a depth-first search
            .map(|(id, pk)| -> Result<_, Error> {
                let mut bin_names = dag.node_weight(id).unwrap().as_schema.bins.names().to_vec();
                bin_names_recursive(&dag, id, &mut bin_names);
                let mut primary_key = Vec::new();
                for key in pk {
                    let idx = bin_names
                        .iter()
                        .position(|bin| bin.to_str().is_ok_and(|bin| bin == key))
                        .ok_or_else(|| Error::FieldNotFound(key.clone()))?;
                    primary_key.push(idx);
                }
                Ok((id, bin_names, primary_key))
            })
            .try_collect()?;
        Ok(Self {
            dag,
            current_transaction: None,
            base_tables,
            transaction_counter: 0,
        })
    }

    fn build_dag(tables: &[(AerospikeSinkTable, Schema)]) -> Result<DenormDag, Error> {
        let mut dag: daggy::Dag<Node, Edge> = daggy::Dag::new();
        let mut node_by_name = HashMap::new();
        for (table, schema) in tables.iter() {
            let bin_names = BinNames::new(schema.fields.iter().map(|field| field.name.as_str()))?;
            let denormalize_to = table
                .write_denormalized_to
                .as_ref()
                .map(|to| -> Result<_, Error> {
                    let AerospikeSet {
                        namespace,
                        set,
                        primary_key,
                    } = to;
                    Ok((
                        CString::new(namespace.as_str())?,
                        CString::new(set.as_str())?,
                        primary_key.clone(),
                    ))
                })
                .transpose()?;
            let idx = dag.add_node(Node {
                as_schema: AerospikeSchema {
                    namespace: CString::new(table.namespace.as_str())?,
                    set: CString::new(table.set_name.as_str())?,
                    bins: bin_names,
                },
                schema: schema.clone(),
                batch: if table.aggregate_by_pk {
                    CachedBatch::Many(OneToManyBatch::default())
                } else {
                    CachedBatch::One(OneToOneBatch::default())
                },
                denormalize_to,
            });

            if node_by_name
                .insert((table.namespace.clone(), table.set_name.clone()), idx)
                .is_some()
            {
                return Err(Error::DuplicateSinkTable {
                    namespace: table.namespace.clone(),
                    set: table.set_name.clone(),
                });
            }
        }
        for (table, schema) in tables {
            let to_idx = node_by_name[&(table.namespace.clone(), table.set_name.clone())];

            for denorm in &table.denormalize {
                let from_idx = node_by_name
                    .get(&(denorm.from_namespace.clone(), denorm.from_set.clone()))
                    .copied()
                    .ok_or_else(|| Error::SetNotFound {
                        namespace: denorm.from_namespace.clone(),
                        set: denorm.from_set.clone(),
                    })?;

                let from_schema = &dag.node_weight(from_idx).unwrap().schema;
                let key_idx = match &denorm.key {
                    dozer_types::models::sink::DenormKey::Simple(name) => {
                        vec![
                            schema
                                .get_field_index(name)
                                .map_err(|_| Error::FieldNotFound(name.to_owned()))?
                                .0,
                        ]
                    }
                    dozer_types::models::sink::DenormKey::Composite(names) => names
                        .iter()
                        .map(|name| {
                            schema
                                .get_field_index(name)
                                .map(|(i, _)| i)
                                .map_err(|_| Error::FieldNotFound(name.to_owned()))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                };
                let mismatch_err = || Error::MismatchedKeys {
                    lookup_namespace: denorm.from_namespace.clone(),
                    lookup_set: denorm.from_set.clone(),
                    denorm_namespace: table.namespace.clone(),
                    denorm_set: table.set_name.clone(),
                };
                if key_idx.len() != from_schema.primary_index.len() {
                    return Err(mismatch_err());
                }
                for (denorm_idx, lookup_idx) in key_idx.iter().zip(&from_schema.primary_index) {
                    let denorm_field = &schema.fields[*denorm_idx];
                    let lookup_field = &from_schema.fields[*lookup_idx];
                    if denorm_field.typ != lookup_field.typ
                        || denorm_field.nullable != lookup_field.nullable
                    {
                        return Err(mismatch_err());
                    }
                }

                let bin_names = BinNames::new(denorm.columns.iter().map(|col| {
                    let (_, dst) = col.to_src_dst();
                    dst
                }))?;

                let bin_indices: Vec<_> = denorm
                    .columns
                    .iter()
                    .map(|col| -> Result<_, Error> {
                        let (src, _) = col.to_src_dst();
                        let (id, _) = from_schema
                            .get_field_index(src)
                            .map_err(|_| Error::FieldNotFound(src.to_owned()))?;
                        Ok(id)
                    })
                    .try_collect()?;

                dag.add_edge(
                    to_idx,
                    from_idx,
                    Edge {
                        key_fields: key_idx,
                        bins: bin_names,
                        field_indices: bin_indices,
                    },
                )
                .map_err(|_| Error::Cycle {
                    namespace: table.namespace.clone(),
                    set: table.set_name.clone(),
                    from_namespace: denorm.from_namespace.clone(),
                    from_set: denorm.from_set.clone(),
                })?;
            }
        }

        Ok(dag)
    }
}

#[derive(Clone)]
struct BatchLookup {
    node: NodeIndex,
    nodebatch_idx: usize,
    version: usize,
    readbatch_idx: Option<usize>,
    follow: bool,
}

impl DenormalizationState {
    fn do_insert(&mut self, node_id: NodeIndex, new: Record) {
        let node = self.dag.node_weight_mut(node_id).unwrap();
        let idx = new.get_key_fields(&node.schema);

        node.batch
            .insert_local(idx, new.values, self.transaction_counter);
    }

    pub(crate) fn process(&mut self, op: TableOperation) -> Result<(), AerospikeSinkError> {
        self.current_transaction = op.id.map(|id| id.txid);
        let node_id: NodeIndex = (op.port as u32).into();
        match op.op {
            dozer_types::types::Operation::Delete { old } => {
                let node = self.dag.node_weight_mut(node_id).unwrap();
                let schema = &node.schema;
                let idx = old.get_key_fields(schema);
                node.batch
                    .remove_local(idx, &old.values, self.transaction_counter);
            }
            dozer_types::types::Operation::Insert { new } => {
                self.do_insert(node_id, new);
            }
            dozer_types::types::Operation::Update { old, new } => {
                let node = self.dag.node_weight_mut(node_id).unwrap();
                let schema = &node.schema;
                let old_pk = old.get_key_fields(schema);
                let new_pk = new.get_key_fields(schema);
                if old_pk != new_pk {
                    return Err(AerospikeSinkError::PrimaryKeyChanged {
                        old: old_pk.clone(),
                        new: new_pk.clone(),
                    });
                }
                node.batch
                    .replace_local(new_pk, old.values, new.values, self.transaction_counter);
            }
            dozer_types::types::Operation::BatchInsert { new } => {
                for value in new {
                    self.do_insert(node_id, value);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        for node in self.dag.node_weights_mut() {
            node.batch.clear();
        }
    }
    pub(crate) fn persist(&mut self, client: &Client) -> Result<(), AerospikeSinkError> {
        let mut read_batch = ReadBatch::new(client, 0, None);
        let mut lookups = Vec::new();
        self.add_manynode_base_lookups(&mut read_batch, &mut lookups)?;
        let read_results = read_batch.execute()?;
        for lookup in lookups {
            self.update_from_lookup(
                lookup.readbatch_idx.unwrap(),
                lookup.node,
                &read_results,
                lookup.nodebatch_idx,
            )?;
        }
        let batch_size_upper_bound: usize = self
            .dag
            .node_references()
            .map(|(_, node)| node.batch.len())
            .sum();

        let batch_size: u32 = batch_size_upper_bound.try_into().unwrap();
        let mut write_batch = WriteBatch::new(client, batch_size, None);

        for node in self.dag.node_weights_mut() {
            // Only write if the last version is dirty (the newest version was changed by this
            // batch)
            node.batch.write(&mut write_batch, &node.as_schema)?;
        }

        write_batch.execute()?;
        self.transaction_counter = 0;
        Ok(())
    }

    pub(crate) fn perform_denorm(
        &mut self,
        client: &Client,
    ) -> Result<Vec<DenormalizedTable>, AerospikeSinkError> {
        let mut lookups = Vec::new();
        for (nid, _, _) in &self.base_tables {
            let node = self.node(*nid);
            let node_keys = node.batch.iter_dirty().map(
                |DirtyRecord {
                     idx,
                     key: _,
                     version,
                 }| BatchLookup {
                    version,
                    node: *nid,
                    nodebatch_idx: idx,
                    readbatch_idx: None,
                    follow: true,
                },
            );
            lookups.extend(node_keys);
        }

        let mut n_lookups = 0;
        let mut batch = ReadBatch::new(client, 0, None);
        while !lookups.is_empty() {
            let batch_results = batch.execute()?;
            let mut new_lookups = Vec::with_capacity(lookups.len());
            let mut new_batch = ReadBatch::new(client, lookups.len().try_into().unwrap(), None);

            // For persisting, we need all many-node baselines, so put them in the
            // first batch
            if n_lookups == 0 {
                self.add_manynode_base_lookups(&mut new_batch, &mut new_lookups)?;
            }
            for BatchLookup {
                node: nid,
                nodebatch_idx,
                version,
                readbatch_idx,
                follow,
            } in lookups
            {
                // Update the node's local batch
                if let Some(readbatch_idx) = readbatch_idx {
                    self.update_from_lookup(readbatch_idx, nid, &batch_results, nodebatch_idx)?;
                }
                if !follow {
                    continue;
                }
                let Some(values) = self.node(nid).batch.get_index(nodebatch_idx, version) else {
                    continue;
                };
                let values = values.collect_vec();
                let mut edges = self
                    .dag
                    .neighbors_directed(nid, Direction::Outgoing)
                    .detach();
                while let Some((edge, target)) = edges.next(self.dag.graph()) {
                    for value in &values {
                        let key = self
                            .edge(edge)
                            .key_fields
                            .iter()
                            .copied()
                            .map(|i| value[i].clone())
                            .collect_vec();
                        let (should_update, batch_idx) = self
                            .dag
                            .node_weight_mut(target)
                            .unwrap()
                            .batch
                            .should_update_at(key.clone(), version);
                        let target_schema = &self.node(target).as_schema;
                        let batch_read_index = if should_update {
                            Some(new_batch.add_read_all(
                                &target_schema.namespace,
                                &target_schema.set,
                                &key,
                            )?)
                        } else {
                            None
                        };

                        new_lookups.push(BatchLookup {
                            node: target,
                            nodebatch_idx: batch_idx,
                            version,
                            readbatch_idx: batch_read_index,
                            follow: true,
                        })
                    }
                }
            }
            lookups = new_lookups;
            batch = new_batch;
            n_lookups += 1;
        }

        let mut res = Vec::new();
        // Recursively collect results
        for (nid, bin_names, pk) in &self.base_tables {
            let mut results = Vec::new();
            let node = self.node(*nid);
            for DirtyRecord {
                idx: _,
                key,
                version,
            } in node.batch.iter_dirty()
            {
                let field_indices = (0..node.schema.fields.len()).collect_vec();
                results.append(&mut self.recurse_lookup(&field_indices, *nid, key, version))
            }
            let (namespace, set, _) = node.denormalize_to.clone().unwrap();
            res.push(DenormalizedTable {
                bin_names: bin_names.clone(),
                namespace,
                set,
                records: results,
                pk: pk.clone(),
            })
        }
        Ok(res)
    }

    fn add_manynode_base_lookups(
        &mut self,
        read_batch: &mut ReadBatch<'_>,
        lookups: &mut Vec<BatchLookup>,
    ) -> Result<(), AerospikeSinkError> {
        for (i, node) in self.dag.node_references() {
            if let CachedBatch::Many(node_batch) = &node.batch {
                for (batch_idx, key) in node_batch
                    .0
                    .iter()
                    .enumerate()
                    .filter_map(|(i, (key, entry))| entry.base.is_none().then_some((i, key)))
                {
                    let batch_read_index = read_batch.add_read_all(
                        &node.as_schema.namespace,
                        &node.as_schema.set,
                        key,
                    )?;
                    lookups.push(BatchLookup {
                        node: i,
                        nodebatch_idx: batch_idx,
                        version: 0,
                        readbatch_idx: Some(batch_read_index),
                        follow: false,
                    });
                }
            }
        }
        Ok(())
    }

    fn update_from_lookup(
        &mut self,
        readbatch_idx: usize,
        nid: NodeIndex,
        batch_results: &ReadBatchResults,
        nodebatch_idx: usize,
    ) -> Result<(), AerospikeSinkError> {
        let node = self.dag.node_weight_mut(nid).unwrap();
        let rec = batch_results.get(readbatch_idx)?;
        match &mut node.batch {
            CachedBatch::One(batch) => batch.insert_remote(
                nodebatch_idx,
                rec.map(|rec| -> Result<_, Error> {
                    parse_record(rec, &node.schema, &node.as_schema.bins)
                })
                .transpose()?,
            ),
            CachedBatch::Many(batch) => batch.insert_remote(
                nodebatch_idx,
                rec.map(|rec| -> Result<_, Error> {
                    parse_record_many(rec, &node.schema, MANY_LIST_BIN, &node.as_schema.bins)
                })
                .transpose()?
                .unwrap_or_default(),
            ),
        }
        Ok(())
    }

    fn recurse_lookup(
        &self,
        field_indices: &[usize],
        node_id: NodeIndex,
        key: &[Field],
        version: usize,
    ) -> Vec<Vec<Field>> {
        let node = self.node(node_id);
        let records = {
            match node.batch.get(key, version) {
                Some(t) => Either::Right(t),
                None => Either::Left(std::iter::once(vec![Field::Null; node.schema.fields.len()])),
            }
        };

        let mut result = Vec::new();
        for record in records {
            let mut results_per_edge = Vec::new();
            for edge in self.dag.edges_directed(node_id, Direction::Outgoing) {
                let key = edge
                    .weight()
                    .key_fields
                    .iter()
                    .map(|i| record[*i].clone())
                    .collect_vec();
                let edge_results =
                    self.recurse_lookup(&edge.weight().field_indices, edge.target(), &key, version);
                results_per_edge.push(edge_results);
            }

            let mut record_result = vec![field_indices
                .iter()
                .map(|i| record[*i].clone())
                .collect_vec()];
            for edge_result in results_per_edge {
                let mut new_record_result = Vec::new();
                for record in record_result {
                    for additional_fields in &edge_result {
                        let mut new_record = record.clone();
                        new_record.extend_from_slice(additional_fields);
                        new_record_result.push(new_record);
                    }
                }
                record_result = new_record_result;
            }
            result.append(&mut record_result);
        }
        result
    }

    pub(crate) fn commit(&mut self) {
        self.transaction_counter += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use dozer_types::{
        models::sink::{
            AerospikeDenormalizations, AerospikeSet, AerospikeSinkTable, DenormColumn, DenormKey,
        },
        rust_decimal::Decimal,
        types::{
            Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
            TableOperation,
        },
    };

    use crate::{aerospike::Client, denorm_dag::DenormalizedTable};

    use super::DenormalizationState;

    macro_rules! schema_row {
        ($schema:expr, $f:literal: $t:ident PRIMARY_KEY) => {
            $schema.field(
                FieldDefinition::new($f.into(), FieldType::$t, true, SourceDefinition::Dynamic),
                true,
            );
        };

        ($schema:expr, $f:literal: $t:ident) => {
            $schema.field(
                FieldDefinition::new($f.into(), FieldType::$t, true, SourceDefinition::Dynamic),
                false,
            );
        };
    }
    macro_rules! schema {
        ($($f:literal: $t:ident $($pk:ident)?),+) => {{
            let mut schema = Schema::new();
            $(schema_row!(schema, $f: $t $($pk)?));+;
            schema
        }};
    }

    #[test]
    #[ignore]
    fn test_denorm() {
        let customer_schema = schema! {
            "id": String PRIMARY_KEY,
            "phone_number": String
        };

        // account to many customer mapping
        let account_owners_schema = schema! {
            "account_id": UInt PRIMARY_KEY,
            "customer_id": String,
            "transaction_limit": UInt
        };

        let transaction_schema = schema! {
            "id": UInt PRIMARY_KEY,
            "account_id": UInt,
            "amount": Decimal
        };

        let tables = vec![
            (
                AerospikeSinkTable {
                    source_table_name: "".into(),
                    namespace: "test".into(),
                    set_name: "customers".into(),
                    denormalize: vec![],
                    write_denormalized_to: None,
                    primary_key: vec!["id".into()],
                    aggregate_by_pk: true,
                },
                customer_schema,
            ),
            (
                AerospikeSinkTable {
                    source_table_name: "".into(),
                    namespace: "test".into(),
                    set_name: "accounts".into(),
                    denormalize: vec![AerospikeDenormalizations {
                        from_namespace: "test".into(),
                        from_set: "customers".into(),
                        key: DenormKey::Simple("customer_id".into()),
                        columns: vec![DenormColumn::Direct("phone_number".into())],
                    }],
                    write_denormalized_to: None,
                    primary_key: vec!["account_id".into()],
                    aggregate_by_pk: false,
                },
                account_owners_schema,
            ),
            (
                AerospikeSinkTable {
                    source_table_name: "".into(),
                    namespace: "test".into(),
                    set_name: "transactions".into(),
                    denormalize: vec![AerospikeDenormalizations {
                        from_namespace: "test".into(),
                        from_set: "accounts".into(),
                        key: DenormKey::Simple("account_id".into()),
                        columns: vec![DenormColumn::Direct("transaction_limit".into())],
                    }],
                    write_denormalized_to: Some(AerospikeSet {
                        namespace: "test".into(),
                        set: "transactions_denorm".into(),
                        primary_key: vec!["id".into(), "phone_number".into()],
                    }),
                    primary_key: vec!["id".into()],
                    aggregate_by_pk: false,
                },
                transaction_schema,
            ),
        ];
        let mut state = DenormalizationState::new(&tables).unwrap();
        // Customers
        state
            .process(TableOperation {
                id: None,
                op: Operation::Insert {
                    new: dozer_types::types::Record::new(vec![
                        Field::String("1001".into()),
                        Field::String("+1234567".into()),
                    ]),
                },
                port: 0,
            })
            .unwrap();
        // Accounts
        state
            .process(TableOperation {
                id: None,
                op: Operation::Insert {
                    new: dozer_types::types::Record::new(vec![
                        Field::UInt(101),
                        Field::String("1001".into()),
                        Field::Null,
                    ]),
                },
                port: 1,
            })
            .unwrap();
        // Transactions
        state
            .process(TableOperation {
                id: None,
                op: Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(1),
                        Field::UInt(101),
                        Field::Decimal(Decimal::new(123, 2)),
                    ]),
                },
                port: 2,
            })
            .unwrap();
        let client = Client::new(&CString::new("localhost:3000").unwrap()).unwrap();
        let res = state.perform_denorm(&client).unwrap();
        assert_eq!(
            res,
            vec![DenormalizedTable {
                bin_names: vec![
                    CString::new("id").unwrap(),
                    CString::new("account_id").unwrap(),
                    CString::new("amount").unwrap(),
                    CString::new("transaction_limit").unwrap(),
                    CString::new("phone_number").unwrap(),
                ],
                records: vec![vec![
                    Field::UInt(1),
                    Field::UInt(101),
                    Field::Decimal(Decimal::new(123, 2)),
                    Field::Null,
                    Field::String("+1234567".into())
                ]],
                namespace: CString::new("test").unwrap(),
                set: CString::new("transactions_denorm").unwrap(),
                pk: vec![0, 4],
            }]
        );
        state.commit();
        state.persist(&client).unwrap();
        state
            .process(TableOperation {
                id: None,
                op: Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(2),
                        Field::UInt(101),
                        Field::Decimal(Decimal::new(321, 2)),
                    ]),
                },
                port: 2,
            })
            .unwrap();
        state.commit();
        state
            .process(TableOperation {
                id: None,
                op: Operation::Update {
                    old: dozer_types::types::Record::new(vec![
                        Field::String("1001".into()),
                        Field::String("+1234567".into()),
                    ]),
                    new: dozer_types::types::Record::new(vec![
                        Field::String("1001".into()),
                        Field::String("+7654321".into()),
                    ]),
                },
                port: 0,
            })
            .unwrap();
        state
            .process(TableOperation {
                id: None,
                op: Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(3),
                        Field::UInt(101),
                        Field::Decimal(Decimal::new(123, 2)),
                    ]),
                },
                port: 2,
            })
            .unwrap();
        state
            .process(TableOperation {
                id: None,
                op: Operation::Insert {
                    new: dozer_types::types::Record::new(vec![
                        Field::String("1001".into()),
                        Field::String("+2 123".into()),
                    ]),
                },
                port: 0,
            })
            .unwrap();
        state.commit();
        let res = state.perform_denorm(&client).unwrap();
        assert_eq!(
            res,
            vec![DenormalizedTable {
                bin_names: vec![
                    CString::new("id").unwrap(),
                    CString::new("account_id").unwrap(),
                    CString::new("amount").unwrap(),
                    CString::new("transaction_limit").unwrap(),
                    CString::new("phone_number").unwrap(),
                ],
                records: vec![
                    vec![
                        Field::UInt(2),
                        Field::UInt(101),
                        Field::Decimal(Decimal::new(321, 2)),
                        Field::Null,
                        Field::String("+1234567".into())
                    ],
                    vec![
                        Field::UInt(3),
                        Field::UInt(101),
                        Field::Decimal(Decimal::new(123, 2)),
                        Field::Null,
                        Field::String("+7654321".into())
                    ],
                    vec![
                        Field::UInt(3),
                        Field::UInt(101),
                        Field::Decimal(Decimal::new(123, 2)),
                        Field::Null,
                        Field::String("+2 123".into())
                    ]
                ],
                namespace: CString::new("test").unwrap(),
                set: CString::new("transactions_denorm").unwrap(),
                pk: vec![0, 4],
            }]
        );
    }
}
