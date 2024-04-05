pub use crate::aerospike::Client;

use aerospike_client_sys::*;
use constants::SNAPSHOT_DEFAULT_BATCH_SIZE;
use crossbeam_channel::Receiver;
use denorm_dag::{AerospikeSchema, DenormalizationState, MANY_LIST_BIN};
use dozer_core::event::EventHub;
use dozer_types::log::error;
use dozer_types::models::connection::AerospikeConnection;
use dozer_types::models::sink::AerospikeSinkTable;
use dozer_types::node::OpIdentifier;
use dozer_types::thiserror;
use dozer_types::types::{OperationKind, Record};
use itertools::Itertools;

use std::collections::HashMap;
use std::ffi::{CStr, CString, NulError};
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::thread::available_parallelism;

use crate::aerospike::{AerospikeError, WriteBatch};

mod aerospike;
mod denorm_dag;

use dozer_core::node::{PortHandle, Sink, SinkFactory};

use dozer_types::errors::internal::BoxedError;
use dozer_types::tonic::async_trait;
use dozer_types::{
    errors::types::TypeError,
    log::warn,
    models::sink::AerospikeSinkConfig,
    types::{Field, FieldType, Schema, TableOperation},
};

mod constants {
    use std::ffi::CStr;

    // TODO: Replace with cstring literals when they're stablized,
    // currently planned for Rust 1.77
    const fn cstr(value: &'static [u8]) -> &'static CStr {
        // Check that the supplied value is valid (ends with nul byte)
        assert!(CStr::from_bytes_with_nul(value).is_ok());
        // Do the conversion again
        unsafe { CStr::from_bytes_with_nul_unchecked(value) }
    }

    pub(super) const META_KEY: &CStr = cstr(b"metadata\0");
    pub(super) const META_BASE_TXN_ID_BIN: &CStr = cstr(b"txn_id\0");
    pub(super) const META_LOOKUP_TXN_ID_BIN: &CStr = cstr(b"txn_id\0");

    pub(super) const SNAPSHOT_DEFAULT_BATCH_SIZE: u32 = 20_000;
}

#[derive(thiserror::Error, Debug)]
enum AerospikeSinkError {
    #[error("Aerospike client error: {0}")]
    Aerospike(#[from] AerospikeError),
    #[error("No primary key found. Aerospike requires records to have a primary key")]
    NoPrimaryKey,
    #[error("Unsupported type for primary key: {0}")]
    UnsupportedPrimaryKeyType(FieldType),
    #[error("Type error: {0}")]
    TypeError(#[from] TypeError),
    #[error("String with internal NUL byte")]
    NulError(#[from] NulError),
    #[error("Could not create record")]
    CreateRecordError,
    #[error("Column name \"{}\" exceeds aerospike's maximum bin name length ({})", .0, AS_BIN_NAME_MAX_LEN)]
    BinNameTooLong(String),
    #[error("Integer out of range. The supplied usigned integer was larger than the maximum representable value for an aerospike integer")]
    IntegerOutOfRange(u64),
    #[error("Changing the value of a primary key is not supported for Aerospike sink. Old: {old:?}, new: {new:?}")]
    PrimaryKeyChanged { old: Vec<Field>, new: Vec<Field> },
    #[error("Denormalization error: {0}")]
    DenormError(#[from] denorm_dag::Error),
    #[error("Inconsistent txid. Denormalized: {denorm:?}, lookup {lookup:?}")]
    InconsistentTxids {
        denorm: Option<u64>,
        lookup: Option<u64>,
    },
    #[error("Unsupported operation during snapshotting: {0}")]
    UnsupportedSnapshotOperation(OperationKind),
}

#[derive(Debug)]
pub struct AerospikeSinkFactory {
    connection_config: AerospikeConnection,
    config: AerospikeSinkConfig,
}

impl AerospikeSinkFactory {
    pub fn new(connection_config: AerospikeConnection, config: AerospikeSinkConfig) -> Self {
        Self {
            connection_config,
            config,
        }
    }
}

#[async_trait]
impl SinkFactory for AerospikeSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        (0..self.config.tables.len() as PortHandle).collect()
    }

    fn get_input_port_name(&self, port: &PortHandle) -> String {
        self.config.tables[*port as usize].source_table_name.clone()
    }

    fn prepare(&self, input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        debug_assert!(input_schemas.len() == self.config.tables.len());
        Ok(())
    }

    async fn build(
        &self,
        mut input_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn dozer_core::node::Sink>, BoxedError> {
        let hosts = CString::new(self.connection_config.hosts.as_str())?;
        let client = Arc::new(Client::new(&hosts).map_err(AerospikeSinkError::from)?);

        let tables: Vec<_> = self
            .config
            .tables
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, table)| -> Result<_, TypeError> {
                let mut schema = input_schemas.remove(&(i as PortHandle)).unwrap();
                if !table.primary_key.is_empty() {
                    let fields = table
                        .primary_key
                        .iter()
                        .map(|key| schema.get_field_index(key))
                        .map_ok(|(i, _)| i)
                        .try_collect()?;
                    schema.primary_index = fields;
                }
                Ok((table, schema))
            })
            .try_collect()?;
        // Validate schemas
        for (_, schema) in tables.iter() {
            if schema.primary_index.is_empty() {
                return Err(AerospikeSinkError::NoPrimaryKey.into());
            };
            for idx in schema.primary_index.iter() {
                match schema.fields[*idx].typ  {
                // These are definitely OK as the primary key
                dozer_types::types::FieldType::UInt
                | dozer_types::types::FieldType::U128
                | dozer_types::types::FieldType::Int
                | dozer_types::types::FieldType::I128
                | dozer_types::types::FieldType::String
                | dozer_types::types::FieldType::Text
                | dozer_types::types::FieldType::Duration
                | dozer_types::types::FieldType::Binary => {}

                // These are OK because we convert them to strings, so warn about
                // them to make sure the user is aware
                typ @ (dozer_types::types::FieldType::Decimal |
                dozer_types::types::FieldType::Timestamp |
                dozer_types::types::FieldType::Date) => warn!("Using a {typ} column as a primary key for Aerospike sink. This is only allowed because this type is converted to a String. Cast to a type supported by aerospike to silence this warning."),

                // These are not OK as keys, so error out
                typ @ (dozer_types::types::FieldType::Float|
                dozer_types::types::FieldType::Boolean |
                dozer_types::types::FieldType::Json |
                dozer_types::types::FieldType::Point ) =>  {
                        return Err(Box::new(AerospikeSinkError::UnsupportedPrimaryKeyType(typ)));
                    }
            }
                for field in &schema.fields {
                    if field.name.len() > AS_BIN_NAME_MAX_LEN as usize {
                        return Err(
                            AerospikeSinkError::BinNameTooLong(field.name.to_owned()).into()
                        );
                    }
                }
            }
        }
        let denorm_state = DenormalizationState::new(&tables)?;

        let metadata_namespace = CString::new(self.config.metadata_namespace.clone())?;
        let metadata_set = CString::new(
            self.config
                .metadata_set
                .to_owned()
                .unwrap_or("__replication_metadata".to_owned()),
        )?;
        let n_threads = self
            .config
            .n_threads
            .or_else(|| available_parallelism().ok())
            .unwrap_or(NonZeroUsize::new(4).unwrap());
        let snapshot_writer_factory = SnapshotWriterFactory::new(
            client.clone(),
            self.config
                .snapshot_batch_size
                .or_else(|| self.config.preferred_batch_size?.try_into().ok())
                .unwrap_or(SNAPSHOT_DEFAULT_BATCH_SIZE),
            &tables,
            n_threads,
        )?;

        Ok(Box::new(AerospikeSink::new(
            self.config.clone(),
            client,
            denorm_state,
            snapshot_writer_factory,
            metadata_namespace,
            metadata_set,
        )?))
    }

    fn type_name(&self) -> String {
        "aerospike".to_string()
    }
}

// A wrapper type responsible for cleaning up a key. This doesn't own an as_key
// instance, as that would involve moving it, while an initialized as_key might
// be self-referential
struct Key<'a>(&'a mut as_key);

impl Key<'_> {
    fn as_ptr(&self) -> *const as_key {
        (&*self.0) as *const as_key
    }
}

impl Drop for Key<'_> {
    fn drop(&mut self) {
        let ptr = self.0 as *mut as_key;
        unsafe { as_key_destroy(ptr) }
    }
}

// A wrapper type responsible for cleaning up a record. This doesn't own an as_record
// instance, as that would involve moving it, while an initialized as_record might
// be self-referential
struct AsRecord<'a>(&'a mut as_record);

impl AsRecord<'_> {
    fn as_ptr(&self) -> *const as_record {
        &*self.0 as *const as_record
    }
}

impl Drop for AsRecord<'_> {
    fn drop(&mut self) {
        let ptr = self.0 as *mut as_record;
        unsafe { as_record_destroy(ptr) }
    }
}

#[derive(Debug)]
struct AerospikeSink {
    config: AerospikeSinkConfig,
    replication_worker: AerospikeSinkWorker,
    metadata_namespace: CString,
    metadata_set: CString,
    client: Arc<Client>,
    snapshot_writer_factory: SnapshotWriterFactory,
}

type TxnId = u64;

#[derive(Debug)]
struct AerospikeMetadata {
    client: Arc<Client>,
    key: NonNull<as_key>,
    record: NonNull<as_record>,
    last_denorm_transaction: Option<u64>,
    last_lookup_transaction: Option<u64>,
}

// NonNull doesn't impl Send
unsafe impl Send for AerospikeMetadata {}

impl AerospikeMetadata {
    fn new(client: Arc<Client>, namespace: CString, set: CString) -> Result<Self, AerospikeError> {
        unsafe {
            let key = NonNull::new(as_key_new(
                namespace.as_ptr(),
                set.as_ptr(),
                constants::META_KEY.as_ptr(),
            ))
            .unwrap();
            let mut record = std::ptr::null_mut();
            #[allow(non_upper_case_globals)]
            let (base, lookup) = match client.get(key.as_ptr(), &mut record) {
                Ok(()) => {
                    let lookup =
                        as_record_get_integer(record, constants::META_LOOKUP_TXN_ID_BIN.as_ptr());
                    let base =
                        as_record_get_integer(record, constants::META_BASE_TXN_ID_BIN.as_ptr());
                    let base = if base.is_null() {
                        None
                    } else {
                        Some((*base).value.try_into().unwrap())
                    };
                    let lookup = if lookup.is_null() {
                        None
                    } else {
                        Some((*lookup).value.try_into().unwrap())
                    };
                    (base, lookup)
                }
                Err(AerospikeError {
                    code: as_status_e_AEROSPIKE_ERR_RECORD_NOT_FOUND,
                    message: _,
                }) => (None, None),
                Err(e) => return Err(e),
            };
            // Not found, so allocate a new record
            if record.is_null() {
                record = as_record_new(2);
            }
            Ok(Self {
                client,
                key,
                record: NonNull::new(record).unwrap(),
                last_denorm_transaction: base,
                last_lookup_transaction: lookup,
            })
        }
    }

    fn write(&mut self, txid: TxnId, bin: &CStr) -> Result<(), AerospikeSinkError> {
        unsafe {
            as_record_set_int64(self.record.as_ptr(), bin.as_ptr(), txid as i64);
            self.client
                .upsert(self.key.as_ptr(), self.record.as_ptr(), None)?;
        }
        Ok(())
    }

    fn write_denorm(&mut self, txid: TxnId) -> Result<(), AerospikeSinkError> {
        self.last_denorm_transaction = Some(txid);
        self.write(txid, constants::META_BASE_TXN_ID_BIN)?;
        Ok(())
    }

    fn write_lookup(&mut self, txid: TxnId) -> Result<(), AerospikeSinkError> {
        self.last_lookup_transaction = Some(txid);
        self.write(txid, constants::META_LOOKUP_TXN_ID_BIN)?;
        Ok(())
    }
}

impl Drop for AerospikeMetadata {
    fn drop(&mut self) {
        unsafe {
            as_record_destroy(self.record.as_ptr());
            as_key_destroy(self.key.as_ptr());
        }
    }
}

impl AerospikeSink {
    fn new(
        config: AerospikeSinkConfig,
        client: Arc<Client>,
        state: DenormalizationState,
        snapshot_writer_factory: SnapshotWriterFactory,
        metadata_namespace: CString,
        metadata_set: CString,
    ) -> Result<Self, AerospikeSinkError> {
        let metadata_writer = AerospikeMetadata::new(
            client.clone(),
            metadata_namespace.clone(),
            metadata_set.clone(),
        )?;

        let worker_instance = AerospikeSinkWorker {
            client: client.clone(),
            state,
            metadata_writer,
            last_committed_transaction: None,
            snapshot_writer: None,
        };

        Ok(Self {
            config,
            replication_worker: worker_instance,
            metadata_namespace,
            metadata_set,
            client,
            snapshot_writer_factory,
        })
    }
}

#[derive(Debug)]
struct AerospikeSinkWorker {
    snapshot_writer: Option<SnapshotWriter>,
    client: Arc<Client>,
    state: DenormalizationState,
    last_committed_transaction: Option<u64>,
    metadata_writer: AerospikeMetadata,
}

#[derive(Debug, Clone)]
struct SetInfo {
    as_schema: AerospikeSchema,
    schema: Schema,
    agg_by_pk: bool,
}

#[derive(Debug)]
struct SnapshotWriterFactory {
    client: Arc<Client>,
    table_infos: Vec<SetInfo>,
    target_batch_size: u32,
    n_threads: NonZeroUsize,
}

#[derive(Debug)]
struct SnapshotWriter {
    target_batch_size: u32,
    batch_size: u32,
    batch: Vec<(usize, Record)>,
    sender: crossbeam_channel::Sender<Vec<(usize, Record)>>,
    receiver: crossbeam_channel::Receiver<Result<(), AerospikeSinkError>>,
}

impl SnapshotWriterFactory {
    fn new(
        client: Arc<Client>,
        batch_size: u32,
        tables: &[(AerospikeSinkTable, Schema)],
        n_threads: NonZeroUsize,
    ) -> Result<Self, AerospikeSinkError> {
        let table_infos = tables
            .iter()
            .map(|(table, schema)| {
                Ok::<_, NulError>(SetInfo {
                    agg_by_pk: table.aggregate_by_pk,
                    as_schema: AerospikeSchema::new(&table.namespace, &table.set_name, schema)?,
                    schema: schema.clone(),
                })
            })
            .try_collect()?;
        Ok(Self {
            table_infos,
            target_batch_size: batch_size,
            client,
            n_threads,
        })
    }

    fn build(&self) -> SnapshotWriter {
        SnapshotWriter::new(
            self.client.clone(),
            self.table_infos.clone(),
            self.target_batch_size,
            self.n_threads,
        )
    }
}

impl SnapshotWriter {
    fn new(
        client: Arc<Client>,
        table_infos: Vec<SetInfo>,
        target_batch_size: u32,
        n_threads: NonZeroUsize,
    ) -> Self {
        let n_threads: usize = n_threads.into();
        let (batch_sender, batch_receiver) =
            crossbeam_channel::bounded(n_threads.saturating_mul(2));
        let (result_sender, result_receiver) =
            crossbeam_channel::bounded(n_threads.saturating_mul(2));
        for _ in 0..n_threads {
            let receiver: Receiver<Vec<(usize, Record)>> = batch_receiver.clone();
            let sender = result_sender.clone();
            let table_infos = table_infos.clone();
            let client = client.clone();
            std::thread::spawn(move || {
                for records in receiver {
                    if sender
                        .send(write_snapshot_batch(&client, records, &table_infos))
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }

        Self {
            target_batch_size,
            batch_size: 0,
            batch: Vec::with_capacity(target_batch_size as usize),
            sender: batch_sender,
            receiver: result_receiver,
        }
    }

    fn process(&mut self, op: TableOperation) -> Result<(), AerospikeSinkError> {
        self.batch_size = self
            .batch_size
            .saturating_add(op.op.len().try_into().unwrap_or(u32::MAX));

        match op.op {
            dozer_types::types::Operation::Insert { new } => {
                self.batch.push((op.port as usize, new))
            }
            dozer_types::types::Operation::BatchInsert { new } => {
                self.batch
                    .extend(new.into_iter().map(|v| (op.port as usize, v)));
            }
            other => {
                return Err(AerospikeSinkError::UnsupportedSnapshotOperation(
                    other.kind(),
                ))
            }
        }

        if self.batch_size >= self.target_batch_size {
            let new_batch = Vec::with_capacity(self.target_batch_size as usize);
            let batch = std::mem::replace(&mut self.batch, new_batch);
            self.sender.send(batch).unwrap();
            self.batch_size = 0;
        }
        if let Ok(Err(e)) = self.receiver.try_recv() {
            return Err(e);
        }
        Ok(())
    }

    fn join(self) -> Result<(), AerospikeSinkError> {
        let final_batch = self.batch;
        self.sender.send(final_batch).unwrap();
        drop(self.sender);
        self.receiver.into_iter().try_collect()?;
        Ok(())
    }
}

fn write_snapshot_batch(
    client: &Arc<Client>,
    records: Vec<(usize, Record)>,
    table_infos: &[SetInfo],
) -> Result<(), AerospikeSinkError> {
    let mut batch = WriteBatch::new(client.clone(), records.len() as u32, None);
    for (i, rec) in records {
        let info = &table_infos[i];
        let key = rec.get_key_fields(&info.schema);
        if info.agg_by_pk {
            batch.add_write_to_list_unique(&info.as_schema, MANY_LIST_BIN, &key, rec.values())?;
        } else {
            batch.add_write(&info.as_schema, &key, rec.values())?;
        }
    }
    batch.execute()?;
    Ok(())
}

impl AerospikeSinkWorker {
    fn process(&mut self, op: TableOperation) -> Result<(), AerospikeSinkError> {
        if let Some(snapshotter) = self.snapshot_writer.as_mut() {
            snapshotter.process(op)?;
        } else {
            self.state.process(op)?;
        }
        Ok(())
    }

    fn commit(&mut self, txid: Option<u64>) -> Result<(), AerospikeSinkError> {
        match (
            txid,
            self.metadata_writer.last_denorm_transaction,
            self.metadata_writer.last_lookup_transaction,
        ) {
                (Some(current), Some(last_denorm), Some(last_lookup)) => {
                    if current <= last_lookup {
                        // We're not caught up so just clear state
                        self.state.clear();
                        return Ok(());
                    }
                    if current <= last_denorm {
                        // Catching up between lookup and denorm. Only need to write lookup.
                        self.state.persist(self.client.clone())?;
                        self.metadata_writer.write_lookup(current)?;
                        return Ok(());
                    }
                    // Else, we're in the normal state and we do the full denorm

                },
                (None, Some(_), None) => {
                    // We are re-snapshotting, because we went down between writing
                    // the base table and writing the lookup tables during the first
                    // transaction after initial snapshotting. Only write the lookup
                    // tables
                    self.state.persist(self.client.clone())?;
                    return Ok(());
                }
                // First transaction. No need to do anything special
                (Some(_) | None, None, None) => {}
                // Base should always be ahead of lookup
                (_, denorm @ None, lookup @ Some(_)) |
                    // If lookup is None, we should be snapshotting and thus have no txid
                    (Some(_), denorm @ Some(_), lookup @ None)|
                    // If we previously had txid's we should always continue to have txid's
                    ( None, denorm @ Some(_), lookup @ Some(_)) => {
                        return Err(AerospikeSinkError::InconsistentTxids { denorm, lookup })
                    }
            }
        self.state.commit();
        self.last_committed_transaction = txid;
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<(), AerospikeSinkError> {
        let txid = self.last_committed_transaction.take();
        let denormalized_tables = self.state.perform_denorm(self.client.clone())?;
        let batch_size_est: usize = denormalized_tables
            .iter()
            .map(|table| table.records.len())
            .sum();
        // Write denormed tables
        let mut batch = WriteBatch::new(self.client.clone(), batch_size_est as u32, None);
        for table in denormalized_tables {
            for record in table.records {
                let key = table.pk.iter().map(|i| record[*i].clone()).collect_vec();
                batch.add_write(&table.as_schema, &key, &record)?;
            }
        }

        batch.execute()?;

        // Write denormed txid
        if let Some(txid) = txid {
            self.metadata_writer.write_denorm(txid)?;
        }

        self.state.persist(self.client.clone())?;

        if let Some(txid) = txid {
            self.metadata_writer.write_lookup(txid)?;
        }
        Ok(())
    }
}

impl Sink for AerospikeSink {
    fn supports_batching(&self) -> bool {
        true
    }

    fn flush_batch(&mut self) -> Result<(), BoxedError> {
        self.replication_worker.flush_batch()?;
        Ok(())
    }

    fn commit(&mut self, epoch_details: &dozer_core::epoch::Epoch) -> Result<(), BoxedError> {
        debug_assert_eq!(epoch_details.common_info.source_states.len(), 1);
        let txid = epoch_details
            .common_info
            .source_states
            .iter()
            .next()
            .and_then(|(_, state)| state.op_id())
            .map(|op_id| op_id.txid);

        self.replication_worker.commit(txid)?;
        Ok(())
    }

    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        self.replication_worker.process(op)?;
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        _connection_name: String,
    ) -> Result<(), BoxedError> {
        self.replication_worker.snapshot_writer = Some(self.snapshot_writer_factory.build());
        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        _connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        if let Some(opid) = id {
            self.replication_worker
                .metadata_writer
                .write_denorm(opid.txid)?;
            self.replication_worker
                .metadata_writer
                .write_lookup(opid.txid)?;
        }
        let writer = self
            .replication_worker
            .snapshot_writer
            .take()
            .expect("No snapshot writer found");
        writer.join()?;
        Ok(())
    }

    fn set_source_state(&mut self, _source_state: &[u8]) -> Result<(), BoxedError> {
        Ok(())
    }

    fn get_source_state(&mut self) -> Result<Option<Vec<u8>>, BoxedError> {
        Ok(None)
    }

    fn get_latest_op_id(&mut self) -> Result<Option<OpIdentifier>, BoxedError> {
        let mut _k = MaybeUninit::uninit();
        let mut _r = std::ptr::null_mut();
        unsafe {
            as_key_init_strp(
                _k.as_mut_ptr(),
                self.metadata_namespace.as_ptr(),
                self.metadata_set.as_ptr(),
                constants::META_KEY.as_ptr(),
                false,
            );
            let key = Key(_k.assume_init_mut());
            #[allow(non_upper_case_globals)]
            match self.client.get(key.as_ptr(), &mut _r) {
                Ok(_) => {}
                Err(AerospikeError {
                    code: as_status_e_AEROSPIKE_ERR_RECORD_NOT_FOUND,
                    message: _,
                }) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
            let record = AsRecord(_r.as_mut().unwrap());
            let txid = as_record_get_int64(
                record.as_ptr(),
                constants::META_LOOKUP_TXN_ID_BIN.as_ptr(),
                -1,
            );
            if txid > 0 {
                Ok(Some(OpIdentifier {
                    txid: txid as u64,
                    seq_in_tx: 0,
                }))
            } else {
                Ok(None)
            }
        }
    }

    fn max_batch_duration_ms(&self) -> Option<u64> {
        self.config.max_batch_duration_ms
    }

    fn preferred_batch_size(&self) -> Option<u64> {
        self.config.preferred_batch_size
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::Arc, time::SystemTime};

    use dozer_core::{epoch::Epoch, tokio};

    use dozer_types::{
        chrono::{DateTime, NaiveDate},
        models::sink::AerospikeSinkTable,
        node::{NodeHandle, SourceStates},
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
        types::{FieldDefinition, Operation, Record},
    };

    use super::*;

    pub(crate) fn client() -> Arc<Client> {
        let client = Client::new(&CString::new("localhost:3000").unwrap()).unwrap();
        let mut response = std::ptr::null_mut();
        let request = "truncate-namespace:namespace=test";
        let request = CString::new(request).unwrap();
        unsafe {
            client.info(&request, &mut response).unwrap();
        }
        client.into()
    }

    fn f(name: &str, typ: FieldType) -> FieldDefinition {
        FieldDefinition {
            name: name.to_owned(),
            typ,
            nullable: false,
            source: dozer_types::types::SourceDefinition::Dynamic,
            description: None,
        }
    }

    const N_RECORDS: usize = 1000000;
    const BATCH_SIZE: usize = 1000;

    #[tokio::test]
    #[ignore]
    async fn test_inserts() {
        let _ = client();
        let mut sink = sink("inserts").await;
        sink.on_source_snapshotting_started("inserts".into())
            .unwrap();
        for i in 0..N_RECORDS {
            sink.process(TableOperation::without_id(
                Operation::Insert {
                    new: record(i as u64),
                },
                0,
            ))
            .unwrap();
        }
        sink.on_source_snapshotting_done("inserts".into(), None)
            .unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_inserts_batch() {
        let _ = client();
        let mut batches = Vec::with_capacity(N_RECORDS / BATCH_SIZE);
        for i in 0..N_RECORDS / BATCH_SIZE {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            for j in (i * BATCH_SIZE)..((i + 1) * BATCH_SIZE) {
                batch.push(record(j as u64));
            }
            batches.push(batch);
        }
        let mut sink = sink("inserts_batch").await;
        sink.on_source_snapshotting_started("inserts".into())
            .unwrap();
        for batch in batches {
            sink.process(TableOperation::without_id(
                Operation::BatchInsert { new: batch },
                0,
            ))
            .unwrap()
        }
        sink.on_source_snapshotting_done("inserts".into(), None)
            .unwrap();
    }

    async fn sink(set: &str) -> Box<dyn Sink> {
        let mut schema = Schema::new();
        schema
            .field(f("uint", FieldType::UInt), true)
            .field(f("int", FieldType::Int), false)
            .field(f("float", FieldType::Float), false)
            .field(f("boolean", FieldType::Boolean), false)
            .field(f("string", FieldType::String), false)
            .field(f("text", FieldType::Text), false)
            .field(f("binary", FieldType::Binary), false)
            .field(f("u128", FieldType::U128), false)
            .field(f("i128", FieldType::I128), false)
            .field(f("decimal", FieldType::Decimal), false)
            .field(f("timestamp", FieldType::Timestamp), false)
            .field(f("date", FieldType::Date), false)
            .field(f("point", FieldType::Point), false)
            .field(f("duration", FieldType::Duration), false)
            .field(
                FieldDefinition {
                    name: "nil".into(),
                    typ: FieldType::UInt,
                    nullable: true,
                    source: dozer_types::types::SourceDefinition::Dynamic,
                    description: None,
                },
                false,
            )
            .field(f("json", FieldType::Json), false);
        let connection_config = AerospikeConnection {
            hosts: "localhost:3000".into(),
            namespace: "test".into(),
            sets: vec![set.to_owned()],
            batching: false,
            ..Default::default()
        };
        let factory = AerospikeSinkFactory::new(
            connection_config,
            AerospikeSinkConfig {
                connection: "".to_owned(),
                n_threads: None,
                tables: vec![AerospikeSinkTable {
                    source_table_name: "test".into(),
                    namespace: "test".into(),
                    set_name: set.to_owned(),
                    denormalize: vec![],
                    write_denormalized_to: None,
                    primary_key: vec![],
                    aggregate_by_pk: true,
                }],
                max_batch_duration_ms: None,
                preferred_batch_size: None,
                metadata_namespace: "test".into(),
                metadata_set: None,
                snapshot_batch_size: None,
            },
        );
        factory
            .build([(0, schema)].into(), EventHub::new(1))
            .await
            .unwrap()
    }

    fn record(i: u64) -> Record {
        Record::new(vec![
            Field::UInt(i | 1),
            Field::Int(i as _),
            Field::Float(OrderedFloat(i as _)),
            Field::Boolean(i % 2 == 0),
            Field::String(i.to_string()),
            Field::Text(i.to_string()),
            Field::Binary(vec![(i % 256) as u8; 1]),
            Field::U128(i as _),
            Field::I128(i as _),
            Field::Decimal(Decimal::new(i as _, 1)),
            Field::Timestamp(DateTime::from_timestamp(i as _, i as _).unwrap().into()),
            Field::Date(NaiveDate::from_num_days_from_ce_opt(i as _).unwrap()),
            /*
                        Field::Point(DozerPoint(Point::new(
                            OrderedFloat((i % 90) as f64),
                            OrderedFloat((i % 90) as f64),
                        ))),
                        Field::Duration(DozerDuration(
                            Duration::from_secs(i),
                            dozer_types::types::TimeUnit::Seconds,
                        )),
            */
            Field::Null,
            /*
                        Field::Json(dozer_types::json_types::json!({
                        i.to_string(): i,
                        i.to_string(): i as f64,
                            "array": vec![i; 5],
                            "object": {
                            "haha": i
                        }
                        })),
            */
        ])
    }

    #[tokio::test]
    #[ignore = "Requires aerospike server"]
    async fn test_txids() {
        let _ = client();
        let mut s = sink("resume").await;
        s.on_source_snapshotting_started("resume".into()).unwrap();
        s.process(TableOperation {
            id: None,
            op: Operation::Insert { new: record(1) },
            port: 0,
        })
        .unwrap();
        s.on_source_snapshotting_done(
            "resume".into(),
            Some(OpIdentifier {
                txid: 6,
                seq_in_tx: 2,
            }),
        )
        .unwrap();
        assert_eq!(
            s.get_latest_op_id().unwrap(),
            Some(OpIdentifier {
                txid: 6,
                seq_in_tx: 0
            })
        );
        s.process(TableOperation {
            id: Some(OpIdentifier {
                txid: 7,
                seq_in_tx: 0,
            }),
            op: Operation::Insert { new: record(7) },
            port: 0,
        })
        .unwrap();

        let mut source_states = SourceStates::new();
        source_states.insert(
            NodeHandle::new(None, "source".into()),
            dozer_types::node::SourceState::Restartable(OpIdentifier {
                txid: 8,
                seq_in_tx: 0,
            }),
        );
        s.commit(&Epoch::new(1, Arc::new(source_states), SystemTime::now()))
            .unwrap();
        s.flush_batch().unwrap();

        assert_eq!(
            s.get_latest_op_id().unwrap(),
            Some(OpIdentifier {
                txid: 8,
                seq_in_tx: 0
            })
        );
        let mut s = sink("resume").await;
        assert_eq!(
            s.get_latest_op_id().unwrap(),
            Some(OpIdentifier {
                txid: 8,
                seq_in_tx: 0
            })
        );
    }
}
