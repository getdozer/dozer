use std::collections::HashMap;
use std::ffi::{CStr, CString, NulError};
use std::ops::Deref;
use std::ptr::{addr_of_mut, NonNull};

use aerospike_client_sys::*;
use dozer_core::daggy::petgraph::Direction;
use dozer_core::daggy::{self, NodeIndex};
use dozer_core::petgraph::visit::{
    EdgeRef, IntoEdgesDirected, IntoNeighborsDirected, IntoNodeReferences,
};
use dozer_types::indexmap::IndexMap;
use dozer_types::log::info;
use dozer_types::models::sink::{AerospikeSet, AerospikeSinkTable};
use dozer_types::thiserror;
use dozer_types::types::{Field, Record, Schema, TableOperation};
use itertools::Itertools;
use smallvec::SmallVec;

use crate::aerospike::{
    as_batch_read_reserve, as_batch_records_create, as_batch_remove_reserve,
    as_batch_write_reserve, as_vector_get, check_alloc, init_batch_write_operations, init_key,
    parse_record, AerospikeError, AsOperations, BinNames, Client,
};
use crate::AerospikeSinkError;

#[derive(Debug, Clone)]
struct CachedRecord {
    dirty: bool,
    version: usize,
    record: Option<Vec<Field>>,
}

impl Deref for CachedRecord {
    type Target = Option<Vec<Field>>;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

#[derive(Debug, Clone)]
struct Node {
    namespace: CString,
    set: CString,
    schema: Schema,
    batch: IndexMap<Vec<Field>, SmallVec<[CachedRecord; 2]>>,
    bins: BinNames,
    denormalize_to: Option<(CString, CString)>,
}

impl Node {
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
        let entry = self.batch.entry(key);
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

    fn insert_remote(&mut self, key: Vec<Field>, value: Option<Vec<Field>>) -> usize {
        self.insert_impl(key, value, 0, false, false)
    }

    fn get(&self, key: &[Field], version: usize) -> Option<&CachedRecord> {
        let versions = self.batch.get(key)?;
        // Find the last version thats <= version
        versions.iter().filter(|v| v.version <= version).last()
    }
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
    base_tables: Vec<(NodeIndex, Vec<CString>)>,
    transaction_counter: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) struct DenormalizedTable {
    pub(crate) bin_names: Vec<CString>,
    pub(crate) namespace: CString,
    pub(crate) set: CString,
    pub(crate) records: Vec<(Vec<Field>, Vec<Field>)>,
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
    pub(crate) fn new(tables: &[(AerospikeSinkTable, Schema)]) -> Result<Self, Error> {
        assert!(!tables.is_empty());
        let dag = Self::build_dag(tables)?;
        let base_tables: Vec<_> = dag
            .node_references()
            // Filter out non-base-tables
            .filter_map(|(i, node)| node.denormalize_to.is_some().then_some(i))
            // Find all added bin names using a depth-first search
            .map(|id| {
                let mut bin_names = dag.node_weight(id).unwrap().bins.names().to_vec();
                bin_names_recursive(&dag, id, &mut bin_names);
                (id, bin_names)
            })
            .collect();
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
            let bin_names = BinNames::new(schema.fields.iter().map(|field| &field.name))?;
            let denormalize_to = table
                .write_denormalized_to
                .as_ref()
                .map(|to| -> Result<_, Error> {
                    let AerospikeSet { namespace, set } = to;
                    Ok((
                        CString::new(namespace.as_str())?,
                        CString::new(set.as_str())?,
                    ))
                })
                .transpose()?;
            let idx = dag.add_node(Node {
                namespace: CString::new(table.namespace.as_str())?,
                set: CString::new(table.set_name.as_str())?,
                schema: schema.clone(),
                batch: IndexMap::new(),
                bins: bin_names,
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

pub(crate) struct RecordBatch {
    inner: NonNull<as_batch_records>,
    allocated_strings: Vec<String>,
    operations: Vec<AsOperations>,
}

impl RecordBatch {
    pub(crate) fn new(capacity: u32, n_strings_estimate: u32) -> Self {
        let ptr = unsafe { NonNull::new(as_batch_records_create(capacity)).unwrap() };
        Self {
            inner: ptr,
            allocated_strings: Vec::with_capacity(n_strings_estimate as usize),
            operations: Vec::with_capacity(capacity as usize),
        }
    }

    pub(crate) unsafe fn as_mut_ptr(&mut self) -> *mut as_batch_records {
        self.inner.as_ptr()
    }

    #[inline(always)]
    pub(crate) unsafe fn inner(&self) -> &as_batch_records {
        self.inner.as_ref()
    }

    pub(crate) fn reserve_read(&mut self) -> *mut as_batch_read_record {
        unsafe { check_alloc(as_batch_read_reserve(self.as_mut_ptr())) }
    }

    pub(crate) fn reserve_write(&mut self) -> *mut as_batch_write_record {
        unsafe { check_alloc(as_batch_write_reserve(self.as_mut_ptr())) }
    }

    pub(crate) fn reserve_remove(&mut self) -> *mut as_batch_remove_record {
        unsafe { check_alloc(as_batch_remove_reserve(self.as_mut_ptr())) }
    }

    pub(crate) fn add_write(
        &mut self,
        namespace: &CStr,
        set: &CStr,
        bin_names: &[CString],
        key: &[Field],
        values: &[Field],
    ) -> Result<(), AerospikeSinkError> {
        let write_rec = self.reserve_write();
        unsafe {
            init_key(
                addr_of_mut!((*write_rec).key),
                namespace,
                set,
                key,
                &mut self.allocated_strings,
            )?;
            let mut ops = AsOperations::new(values.len().try_into().unwrap());
            init_batch_write_operations(
                ops.as_mut_ptr(),
                values,
                bin_names,
                &mut self.allocated_strings,
            )?;
            (*write_rec).ops = ops.as_mut_ptr();
            self.operations.push(ops);
        }
        Ok(())
    }

    fn add_remove(
        &mut self,
        namespace: &CStr,
        set: &CStr,
        key: &[Field],
    ) -> Result<(), AerospikeSinkError> {
        let remove_rec = self.reserve_remove();
        unsafe {
            init_key(
                addr_of_mut!((*remove_rec).key),
                namespace,
                set,
                key,
                &mut self.allocated_strings,
            )?;
        }
        Ok(())
    }

    fn add_read_all(
        &mut self,
        namespace: &CStr,
        set: &CStr,
        key: &[Field],
    ) -> Result<(), AerospikeSinkError> {
        let read_rec = self.reserve_read();
        unsafe {
            init_key(
                addr_of_mut!((*read_rec).key),
                namespace,
                set,
                key,
                &mut self.allocated_strings,
            )?;
            (*read_rec).read_all_bins = true;
        }
        Ok(())
    }
}

impl Drop for RecordBatch {
    fn drop(&mut self) {
        unsafe { as_batch_records_destroy(self.inner.as_ptr()) }
    }
}

#[derive(Clone)]
struct BatchLookup {
    base_table: usize,
    index: usize,
    version: usize,
    key: Option<Vec<Field>>,
    batch_read_index: Option<usize>,
}

impl DenormalizationState {
    fn do_insert(&mut self, node_id: NodeIndex, new: Record) {
        let node = self.dag.node_weight_mut(node_id).unwrap();
        let idx = new.get_key_fields(&node.schema);

        node.insert_local(idx, Some(new.values.clone()), self.transaction_counter);
    }

    pub(crate) fn process(&mut self, op: TableOperation) -> Result<(), AerospikeSinkError> {
        self.current_transaction = op.id.map(|id| id.txid);
        let node_id: NodeIndex = (op.port as u32).into();
        match op.op {
            dozer_types::types::Operation::Delete { old } => {
                let node = self.dag.node_weight_mut(node_id).unwrap();
                let schema = &node.schema;
                let idx = old.get_key_fields(schema);
                node.insert_local(idx, None, self.transaction_counter);
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
                node.insert_local(new_pk, Some(new.values), self.transaction_counter);
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
        let batch_size_upper_bound: usize = self
            .dag
            .node_references()
            .map(|(_, node)| node.batch.len())
            .sum();

        let batch_size: u32 = batch_size_upper_bound.try_into().unwrap();
        info!("Writing denorm batch of size {}", batch_size);
        let mut write_batch = RecordBatch::new(batch_size, batch_size);

        for node in self.dag.node_weights_mut() {
            // Only write if the last version is dirty (the newest version was changed by this
            // batch)
            for (key, dirty_record) in node.batch.drain(..).filter_map(|(key, mut rec)| {
                let last_version = rec.pop()?;
                last_version.dirty.then_some((key, last_version.record))
            }) {
                if let Some(dirty_record) = dirty_record {
                    write_batch.add_write(
                        &node.namespace,
                        &node.set,
                        node.bins.names(),
                        &key,
                        &dirty_record,
                    )?;
                } else {
                    write_batch.add_remove(&node.namespace, &node.set, &key)?;
                }
            }
        }

        unsafe {
            client.write_batch(write_batch.as_mut_ptr())?;
        }
        self.transaction_counter = 0;
        Ok(())
    }

    pub(crate) fn perform_denorm(
        &mut self,
        client: &Client,
    ) -> Result<Vec<DenormalizedTable>, AerospikeSinkError> {
        let mut lookups = Vec::new();
        for (base_table_idx, (nid, _)) in self.base_tables.iter().enumerate() {
            let node = self.dag.node_weight(*nid).unwrap();
            let node_keys = node
                .batch
                .iter()
                .enumerate()
                .filter_map(|(i, (k, v))| Some((i, k, v.last()?)))
                .filter(|(_, _, v)| v.dirty)
                .map(|(i, k, v)| BatchLookup {
                    base_table: base_table_idx,
                    index: i,
                    version: v.version,
                    key: Some(k.clone()),
                    batch_read_index: None,
                })
                .collect_vec();
            let n_cols = node.schema.fields.len();
            lookups.push((*nid, (0..n_cols).collect_vec(), node_keys));
        }
        let mut results: Vec<DenormalizedTable> = self
            .base_tables
            .iter()
            .zip(lookups.iter())
            .map(|((nid, bin_names), (_, _, keys))| {
                let node = self.dag.node_weight(*nid).unwrap();
                let (namespace, set) = node.denormalize_to.clone().unwrap();
                DenormalizedTable {
                    bin_names: bin_names.clone(),
                    namespace,
                    set,
                    records: keys
                        .iter()
                        .map(|key| {
                            (
                                key.key.clone().unwrap(),
                                Vec::with_capacity(bin_names.len()),
                            )
                        })
                        .collect(),
                }
            })
            .collect();

        let mut batch = RecordBatch::new(0, 0);
        while !lookups.is_empty() {
            let mut new_lookups = Vec::new();
            let batch_size: usize = lookups.iter().map(|(_, _, keys)| keys.len()).sum();
            let mut new_batch = RecordBatch::new(batch_size as u32, batch_size as u32);
            let mut batch_idx = 0;
            for (nid, fields, node_lookups) in lookups {
                let node = self.dag.node_weight_mut(nid).unwrap();
                for lookup in node_lookups.iter().cloned() {
                    let BatchLookup {
                        base_table,
                        index,
                        version,
                        key,
                        batch_read_index,
                    } = lookup;
                    // Update the node, if we retrieved from the remote
                    if let Some(batch_read_index) = batch_read_index {
                        unsafe {
                            let rec = as_vector_get(
                                &batch.inner().list as *const as_vector,
                                batch_read_index,
                            ) as *const as_batch_read_record;
                            let result = (*rec).result;
                            if result == as_status_e_AEROSPIKE_ERR_RECORD_NOT_FOUND {
                                node.insert_remote(key.clone().unwrap(), None);
                            } else if result == as_status_e_AEROSPIKE_OK {
                                node.insert_remote(
                                    key.clone().unwrap(),
                                    Some(parse_record(&(*rec).record, &node.schema, &node.bins)?),
                                );
                            } else {
                                return Err(AerospikeError::from_code(result).into());
                            }
                        }
                    }
                    // Look up in node and add to new batch. If it wasn't in there, we looked
                    // it up remotely, so this is always `Some`
                    let record = key.as_ref().map(|key| node.get(key, version).unwrap());
                    let base_fields = &mut results[base_table].records[index].1;
                    if let Some(record) = record.and_then(|record| record.record.as_ref()) {
                        let denorm_fields = fields.iter().copied().map(|i| record[i].clone());
                        base_fields.extend(denorm_fields);
                    } else {
                        base_fields.extend(std::iter::repeat(Field::Null).take(fields.len()));
                    }
                }
                let node = self.dag.node_weight(nid).unwrap();

                for edge in self.dag.edges_directed(nid, Direction::Outgoing) {
                    let mut new_node_lookups = Vec::with_capacity(node_lookups.len());
                    let key_fields = &edge.weight().key_fields;
                    let target_node = self.dag.node_weight(edge.target()).unwrap();
                    for lookup in &node_lookups {
                        let BatchLookup {
                            base_table,
                            index,
                            version,
                            key,
                            batch_read_index: _,
                        } = lookup;
                        let (new_key, new_batch_read_index) = if let Some(record) = key
                            .as_ref()
                            .and_then(|key| node.get(key, *version).unwrap().record.as_ref())
                        {
                            let new_key: Vec<Field> = key_fields
                                .iter()
                                .copied()
                                .map(|i| record[i].clone())
                                .collect();
                            let batch_id = if target_node.get(&new_key, *version).is_none() {
                                new_batch.add_read_all(
                                    &target_node.namespace,
                                    &target_node.set,
                                    &new_key,
                                )?;
                                let idx = batch_idx;
                                batch_idx += 1;
                                Some(idx)
                            } else {
                                None
                            };
                            (Some(new_key), batch_id)
                        } else {
                            (None, None)
                        };
                        new_node_lookups.push(BatchLookup {
                            base_table: *base_table,
                            index: *index,
                            version: *version,
                            key: new_key,
                            batch_read_index: new_batch_read_index,
                        });
                    }
                    new_lookups.push((
                        edge.target(),
                        edge.weight().field_indices.clone(),
                        new_node_lookups,
                    ));
                }
            }
            lookups = new_lookups;
            batch = new_batch;
            if batch_idx > 0 {
                unsafe {
                    client.batch_get(batch.as_mut_ptr())?;
                }
            }
        }
        Ok(results)
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

    #[test]
    #[ignore]
    fn test_denorm() {
        let mut customer_schema = Schema::new();
        customer_schema
            .field(
                FieldDefinition::new(
                    "id".into(),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                true,
            )
            .field(
                FieldDefinition::new(
                    "phone_number".into(),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            );

        let mut account_schema = Schema::new();
        account_schema
            .field(
                FieldDefinition::new(
                    "id".into(),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                true,
            )
            .field(
                FieldDefinition::new(
                    "customer_id".into(),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "transaction_limit".into(),
                    FieldType::UInt,
                    true,
                    SourceDefinition::Dynamic,
                ),
                false,
            );
        let mut transaction_schema = Schema::new();
        transaction_schema
            .field(
                FieldDefinition::new(
                    "id".into(),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                true,
            )
            .field(
                FieldDefinition::new(
                    "account_id".into(),
                    FieldType::UInt,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "amount".into(),
                    FieldType::Decimal,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            );

        let tables = vec![
            (
                AerospikeSinkTable {
                    source_table_name: "".into(),
                    namespace: "test".into(),
                    set_name: "customers".into(),
                    denormalize: vec![],
                    write_denormalized_to: None,
                    primary_key: vec![],
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
                    primary_key: vec![],
                },
                account_schema,
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
                    }),
                    primary_key: vec![],
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
                records: vec![(
                    vec![Field::UInt(1)],
                    vec![
                        Field::UInt(1),
                        Field::UInt(101),
                        Field::Decimal(Decimal::new(123, 2)),
                        Field::Null,
                        Field::String("+1234567".into())
                    ]
                )],
                namespace: CString::new("test").unwrap(),
                set: CString::new("transactions_denorm").unwrap()
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
                    (
                        vec![Field::UInt(2)],
                        vec![
                            Field::UInt(2),
                            Field::UInt(101),
                            Field::Decimal(Decimal::new(321, 2)),
                            Field::Null,
                            Field::String("+1234567".into())
                        ]
                    ),
                    (
                        vec![Field::UInt(3)],
                        vec![
                            Field::UInt(3),
                            Field::UInt(101),
                            Field::Decimal(Decimal::new(123, 2)),
                            Field::Null,
                            Field::String("+7654321".into())
                        ]
                    )
                ],
                namespace: CString::new("test").unwrap(),
                set: CString::new("transactions_denorm").unwrap()
            }]
        );
    }
}
