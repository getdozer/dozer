use std::collections::HashMap;
use std::ffi::{CStr, CString, NulError};
use std::ops::Deref;
use std::ptr::{addr_of_mut, NonNull};

use aerospike_client_sys::*;
use dozer_core::daggy::petgraph::Direction;
use dozer_core::daggy::{self, EdgeIndex, NodeIndex};
use dozer_core::petgraph::visit::{
    EdgeRef, IntoEdgesDirected, IntoNeighborsDirected, IntoNodeReferences,
};
use dozer_types::indexmap::IndexMap;
use dozer_types::models::sink::{AerospikeSet, AerospikeSinkTable};
use dozer_types::thiserror;
use dozer_types::types::{Field, Record, Schema, TableOperation};
use itertools::Itertools;

use crate::aerospike::{
    as_batch_read_reserve, as_batch_records_create, as_batch_remove_reserve,
    as_batch_write_reserve, as_vector_get, check_alloc, init_batch_write_operations, init_key,
    parse_record, AerospikeError, AsOperations, BinNames, Client,
};
use crate::AerospikeSinkError;

#[derive(Debug, Clone)]
struct CachedRecord {
    dirty: bool,
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
    batch: IndexMap<Vec<Field>, CachedRecord>,
    bins: BinNames,
    denormalize_to: Option<(CString, CString)>,
}

impl Node {
    fn insert(&mut self, key: Vec<Field>, value: Option<Vec<Field>>) -> usize {
        self.batch
            .insert_full(
                key,
                CachedRecord {
                    dirty: true,
                    record: value,
                },
            )
            .0
    }

    fn get_index_or_insert(&mut self, key: Vec<Field>, value: Option<Vec<Field>>) -> usize {
        let entry = self.batch.entry(key);
        let index = entry.index();
        entry.or_insert(CachedRecord {
            dirty: true,
            record: value,
        });
        index
    }
}

#[derive(Debug, Clone)]
struct Edge {
    bins: BinNames,
    key_fields: Vec<usize>,
    keys: HashMap<usize, Vec<Field>>,
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
}

#[derive(Debug)]
pub(crate) struct DenormalizationState {
    layers: Vec<Vec<NodeIndex>>,
    dag: DenormDag,
    current_transaction: Option<u64>,
    base_tables: Vec<(NodeIndex, Vec<CString>)>,
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
        let layers = Self::build_layers(dag.clone())?;
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
            layers,
            dag,
            current_transaction: None,
            base_tables,
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
                        keys: HashMap::new(),
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

    fn build_layers(mut dag: DenormDag) -> Result<Vec<Vec<NodeIndex>>, Error> {
        let mut layers = Vec::new();

        while dag.node_count() > 0 {
            // Find all the nodes without incoming edges
            let roots: Vec<_> = dag
                .graph()
                .node_indices()
                .filter(|index| {
                    dag.edges_directed(*index, Direction::Incoming)
                        .next()
                        .is_none()
                })
                .collect();
            for root in &roots {
                dag.remove_node(*root);
            }
            layers.push(roots);
        }

        Ok(layers)
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
        let remove_rec = self.reserve_read();
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
}

impl Drop for RecordBatch {
    fn drop(&mut self) {
        unsafe { as_batch_records_destroy(self.inner.as_ptr()) }
    }
}

impl DenormalizationState {
    fn do_insert(&mut self, node_id: NodeIndex, new: Record) {
        let node = self.dag.node_weight_mut(node_id).unwrap();
        let idx = new.get_key_fields(&node.schema);

        let batch_idx = node.insert(idx, Some(new.values.clone()));
        // Queue a lookup for all the outgoing edges
        let mut edges = self
            .dag
            .neighbors_directed(node_id, Direction::Outgoing)
            .detach();
        while let Some(edge) = edges.next_edge(self.dag.graph()) {
            let edge_weight = self.dag.edge_weight_mut(edge).unwrap();
            let key = edge_weight
                .key_fields
                .iter()
                .map(|i| new.values[*i].clone())
                .collect();
            edge_weight.keys.insert(batch_idx, key);
        }
    }

    pub(crate) fn process(&mut self, op: TableOperation) -> Result<(), AerospikeSinkError> {
        self.current_transaction = op.id.map(|id| id.txid);
        let node_id: NodeIndex = (op.port as u32).into();
        match op.op {
            dozer_types::types::Operation::Delete { old } => {
                let node = self.dag.node_weight_mut(node_id).unwrap();
                let schema = &node.schema;
                let idx = old.get_key_fields(schema);
                node.insert(idx, None);
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
                node.insert(new_pk, Some(new.values));
            }
            dozer_types::types::Operation::BatchInsert { new } => {
                for value in new {
                    self.do_insert(node_id, value);
                }
            }
        }
        Ok(())
    }

    fn build_read_batch(&self, layer: &[NodeIndex]) -> Result<RecordBatch, AerospikeSinkError> {
        let batch_size: u32 = layer
            .iter()
            .flat_map(|node| self.dag.edges_directed(*node, Direction::Incoming))
            .map(|edge| edge.weight().keys.len())
            .sum::<usize>()
            .try_into()
            .unwrap();
        let mut batch = RecordBatch::new(batch_size, batch_size);

        for node_id in layer {
            let node = self.dag.node_weight(*node_id).unwrap();
            for input in self.dag.edges_directed(*node_id, Direction::Incoming) {
                let edge = self.dag.edge_weight(input.id()).unwrap();
                for key in edge.keys.values() {
                    batch.add_read_all(&node.namespace, &node.set, key)?;
                }
            }
        }
        Ok(batch)
    }

    pub(crate) fn persist(&mut self, client: &Client) -> Result<(), AerospikeSinkError> {
        let batch_size_upper_bound: usize = self
            .dag
            .node_references()
            .map(|(_, node)| node.batch.len())
            .sum();

        let batch_size: u32 = batch_size_upper_bound.try_into().unwrap();
        let mut write_batch = RecordBatch::new(batch_size, batch_size);

        for node in self.dag.node_weights_mut() {
            for (key, dirty_record) in node
                .batch
                .drain(..)
                .filter_map(|(i, rec)| rec.dirty.then_some((i, rec.record)))
            {
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
        for edge in self.dag.edge_weights_mut() {
            edge.keys.clear();
        }
        unsafe {
            client.write_batch(write_batch.as_mut_ptr())?;
        }
        Ok(())
    }

    pub(crate) fn perform_denorm(
        &mut self,
        client: &Client,
    ) -> Result<Vec<DenormalizedTable>, AerospikeSinkError> {
        for layer in &self.layers {
            let mut batch = self.build_read_batch(layer)?;
            unsafe {
                client.batch_get(batch.as_mut_ptr())?;
            }
            let mut idx = 0;
            // Do the get and update the cache
            let record_list = unsafe { &batch.inner().list };
            for node_id in layer {
                let mut edges = self
                    .dag
                    .neighbors_directed(*node_id, Direction::Incoming)
                    .detach();
                while let Some(input) = edges.next_edge(self.dag.graph()) {
                    let keys = &self.dag.edge_weight(input).unwrap().keys;
                    let mut vals = Vec::with_capacity(keys.len());
                    for key in keys.values() {
                        unsafe {
                            debug_assert!(idx < record_list.size as usize);
                            let record = as_vector_get(record_list as *const as_vector, idx)
                                as *const as_batch_read_record;
                            let result = (*record).result;
                            let node = self.dag.node_weight(*node_id).unwrap();
                            if result == as_status_e_AEROSPIKE_ERR_RECORD_NOT_FOUND {
                                vals.push((key.clone(), None));
                            } else if result == as_status_e_AEROSPIKE_OK {
                                vals.push((
                                    key.clone(),
                                    Some(parse_record(
                                        &(*record).record,
                                        &node.schema,
                                        &node.bins,
                                    )?),
                                ));
                            } else {
                                return Err(AerospikeError::from_code(result).into());
                            }
                            idx += 1;
                        }
                    }
                    for (key, val) in vals {
                        self.dag
                            .node_weight_mut(*node_id)
                            .unwrap()
                            .get_index_or_insert(key, val);
                    }
                }
                // Update the outgoing edges with the keys from this layer
                let mut edges = self
                    .dag
                    .neighbors_directed(*node_id, Direction::Outgoing)
                    .detach();
                while let Some(edge_id) = edges.next_edge(self.dag.graph()) {
                    let node = self.dag.node_weight(*node_id).unwrap();
                    let edge = self.dag.edge_weight(edge_id).unwrap();
                    let mut keys_to_add = Vec::with_capacity(node.batch.len());
                    for (i, v) in node.batch.values().enumerate() {
                        if let Some(v) = &v.record {
                            let key_values =
                                edge.key_fields.iter().map(|i| v[*i].clone()).collect();
                            keys_to_add.push((i, key_values))
                        }
                    }
                    self.dag
                        .edge_weight_mut(edge_id)
                        .unwrap()
                        .keys
                        .extend(keys_to_add);
                }
            }
        }

        let mut ret = Vec::new();
        // Run through the graph again, now that all the data is in the caches
        for (nid, bin_names) in self.base_tables.iter().cloned() {
            let mut node_batch = self.dag.node_weight(nid).unwrap().batch.clone();
            for (primary_table_index, primary_table_fields) in node_batch
                .values_mut()
                .flat_map(|v| &mut v.record)
                .enumerate()
            {
                for edge in self.dag.edges_directed(nid, Direction::Outgoing) {
                    self.recurse_dag_lookup(
                        primary_table_fields,
                        edge.id(),
                        primary_table_index,
                        false,
                    );
                }
            }
            let node = self.dag.node_weight(nid).unwrap();
            let (namespace, set) = node.denormalize_to.clone().unwrap();
            let table = DenormalizedTable {
                namespace,
                set,
                bin_names,
                records: node_batch
                    .into_iter()
                    .filter_map(|(key, rec)| rec.dirty.then_some((key, rec.record?)))
                    .collect(),
            };
            ret.push(table);
        }

        Ok(ret)
    }

    fn recurse_dag_lookup(
        &self,
        primary_table_fields: &mut Vec<Field>,
        edge_id: EdgeIndex,
        base_id: usize,
        mut fill_null: bool,
    ) {
        let edge = self.dag.edge_weight(edge_id).unwrap();
        let tgt_id = self.dag.edge_endpoints(edge_id).unwrap().1;
        let tgt = self.dag.node_weight(tgt_id).unwrap();
        let key = &edge.keys[&base_id];
        let (tgt_table_index, _tgt_key, tgt_values) = tgt.batch.get_full(key).unwrap();
        if let (Some(lookup_fields), false) = (tgt_values.deref(), fill_null) {
            primary_table_fields
                .extend(edge.field_indices.iter().map(|i| lookup_fields[*i].clone()))
        } else {
            fill_null = true;
            primary_table_fields.extend(std::iter::repeat(Field::Null).take(edge.bins.len()));
        }

        for new_edge in self.dag.edges_directed(tgt_id, Direction::Outgoing) {
            self.recurse_dag_lookup(
                primary_table_fields,
                new_edge.id(),
                tgt_table_index,
                fill_null,
            )
        }
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
    fn test_name() {
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
    }
}
