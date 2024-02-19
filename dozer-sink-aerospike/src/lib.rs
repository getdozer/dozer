use constants::{META_KEY, META_TXN_ID_BIN};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use dozer_types::json_types::{DestructuredJsonRef, JsonValue};
use dozer_types::models::connection::AerospikeConnection;
use dozer_types::models::sink::DenormColumn;
use dozer_types::node::OpIdentifier;
use std::alloc::{handle_alloc_error, Layout};
use std::ffi::{c_char, c_void, CStr, CString, NulError};
use std::fmt::Display;
use std::mem::{self, MaybeUninit};
use std::num::NonZeroUsize;
use std::ptr::{addr_of, addr_of_mut, null, NonNull};
use std::sync::Arc;
use std::thread::{available_parallelism, JoinHandle};
use std::time::{Duration, Instant};
use std::{collections::HashMap, fmt::Debug};

use aerospike_client_sys::*;
use dozer_core::{
    node::{PortHandle, Sink, SinkFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::errors::internal::BoxedError;
use dozer_types::geo::{Coord, Point};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::tonic::async_trait;
use dozer_types::{
    errors::types::TypeError,
    log::{error, info, warn},
    models::sink::AerospikeSinkConfig,
    thiserror::{self, Error},
    types::{
        DozerDuration, DozerPoint, Field, FieldType, Operation, Record, Schema, TableOperation,
    },
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

    pub(super) const TXN_ID_BIN: &CStr = cstr(b"__txn_id\0");
    pub(super) const TXN_SEQ_BIN: &CStr = cstr(b"__txn_seq\0");
    pub(super) const META_KEY: &CStr = cstr(b"metadata\0");
    pub(super) const META_TXN_ID_BIN: &CStr = cstr(b"txn_id\0");
}

#[derive(Error, Debug)]
enum AerospikeSinkError {
    #[error("Aerospike client error: {} - {}", .0.code, .0.message)]
    Aerospike(#[from] AerospikeError),
    #[error("Aerospike does not support composite primary keys")]
    CompositePrimaryKey,
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
    #[error("Changing the value of a primary key is not supported for Aerospike sink. Old: {old}, new: {new}")]
    PrimaryKeyChanged { old: Field, new: Field },
}

#[derive(Debug, Error)]
struct AerospikeError {
    code: i32,
    message: String,
}

impl From<as_error> for AerospikeError {
    fn from(value: as_error) -> Self {
        let code = value.code;
        let message = unsafe {
            let message = CStr::from_ptr(value.message.as_ptr());
            // The message is ASCII (I think?), so this should not fail
            message.to_str().unwrap()
        };
        Self {
            code,
            message: message.to_owned(),
        }
    }
}

impl Display for AerospikeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.code, self.message)
    }
}

// Client should never be `Clone`, because of the custom Drop impl
#[derive(Debug)]
struct Client {
    inner: NonNull<aerospike>,
}

// The aerospike client API is thread-safe.
unsafe impl Send for Client {}
unsafe impl Sync for Client {}

#[inline(always)]
unsafe fn check_alloc<T>(ptr: *mut T) -> *mut T {
    if ptr.is_null() {
        handle_alloc_error(Layout::new::<T>())
    }
    ptr
}
#[inline(always)]
unsafe fn as_try(f: impl FnOnce(*mut as_error) -> as_status) -> Result<(), AerospikeError> {
    let mut err = MaybeUninit::uninit();
    if f(err.as_mut_ptr()) == as_status_e_AEROSPIKE_OK {
        Ok(())
    } else {
        Err(AerospikeError::from(err.assume_init()))
    }
}

impl Client {
    fn new(hosts: &CStr) -> Result<Self, AerospikeError> {
        let mut config = unsafe {
            let mut config = MaybeUninit::uninit();
            as_config_init(config.as_mut_ptr());
            config.assume_init()
        };
        config.policies.batch.base.total_timeout = 10000;
        unsafe {
            // The hosts string will be copied, so pass it as `as_ptr` so the original
            // gets deallocated at the end of this block
            as_config_add_hosts(&mut config as *mut as_config, hosts.as_ptr(), 3000);
        }
        // Allocate a new client instance. Our `Drop` implementation will make
        // sure it is destroyed
        let this = unsafe {
            let inner = aerospike_new(&mut config as *mut as_config);
            if inner.is_null() {
                handle_alloc_error(Layout::new::<aerospike>())
            } else {
                let this = Self {
                    inner: NonNull::new_unchecked(inner),
                };
                this.connect()?;
                this
            }
        };
        Ok(this)
    }

    fn connect(&self) -> Result<(), AerospikeError> {
        unsafe { as_try(|err| aerospike_connect(self.inner.as_ptr(), err)) }
    }

    unsafe fn put(
        &self,
        key: *const as_key,
        record: *mut as_record,
        mut policy: as_policy_write,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        if let Some(filter) = filter {
            policy.base.filter_exp = filter.as_ptr();
        }
        as_try(|err| {
            aerospike_key_put(
                self.inner.as_ptr(),
                err,
                &policy as *const as_policy_write,
                key,
                record,
            )
        })
    }

    unsafe fn insert(
        &self,
        key: *const as_key,
        new: *mut as_record,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        let mut policy = self.inner.as_ref().config.policies.write;
        policy.exists = as_policy_exists_e_AS_POLICY_EXISTS_CREATE;
        self.put(key, new, policy, filter)
    }

    unsafe fn update(
        &self,
        key: *const as_key,
        new: *mut as_record,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        let mut policy = self.inner.as_ref().config.policies.write;
        policy.exists = as_policy_exists_e_AS_POLICY_EXISTS_UPDATE;
        self.put(key, new, policy, filter)
    }

    unsafe fn upsert(
        &self,
        key: *const as_key,
        new: *mut as_record,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        let mut policy = self.inner.as_ref().config.policies.write;
        policy.exists = as_policy_exists_e_AS_POLICY_EXISTS_CREATE_OR_REPLACE;
        self.put(key, new, policy, filter)
    }

    unsafe fn delete(
        &self,
        key: *const as_key,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        let mut policy = self.inner.as_ref().config.policies.remove;
        if let Some(filter) = filter {
            policy.base.filter_exp = filter.as_ptr();
        }
        as_try(|err| {
            aerospike_key_remove(
                self.inner.as_ptr(),
                err,
                &policy as *const as_policy_remove,
                key,
            )
        })
    }

    unsafe fn insert_batch(&self, batch: *mut as_batch_records) -> Result<(), AerospikeError> {
        let policy = self.inner.as_ref().config.policies.batch;

        as_try(|err| {
            aerospike_batch_write(
                self.inner.as_ptr(),
                err,
                &policy as *const as_policy_batch,
                batch,
            )
        })
    }

    unsafe fn select(
        &self,
        key: *const as_key,
        bins: &[*const c_char],
        record: &mut *mut as_record,
    ) -> Result<(), AerospikeError> {
        as_try(|err| {
            aerospike_key_select(
                self.inner.as_ptr(),
                err,
                null(),
                key,
                // This won't write to the mut ptr
                bins.as_ptr() as *mut *const c_char,
                record as *mut *mut as_record,
            )
        })
    }
    unsafe fn get(
        &self,
        key: *const as_key,
        record: &mut *mut as_record,
    ) -> Result<(), AerospikeError> {
        as_try(|err| {
            aerospike_key_get(
                self.inner.as_ptr(),
                err,
                std::ptr::null(),
                key,
                record as *mut *mut as_record,
            )
        })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        unsafe {
            aerospike_destroy(self.inner.as_ptr());
        }
    }
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
    ) -> Result<Box<dyn dozer_core::node::Sink>, BoxedError> {
        let hosts = CString::new(self.connection_config.hosts.as_str())?;
        let client = Client::new(&hosts).map_err(AerospikeSinkError::from)?;
        let n_threads = self
            .config
            .n_threads
            .or_else(|| available_parallelism().ok())
            .unwrap_or_else(|| {
                warn!("Unable to automatically determine the correct amount of threads to use for Aerospike sink, so defaulting to 1.\nTo override, set `n_threads` in your Aerospike sink config");
                NonZeroUsize::new(1).unwrap()
            });

        let mut tables = vec![];
        for (port, table) in self.config.tables.iter().enumerate() {
            let schema = input_schemas.remove(&(port as PortHandle)).unwrap();
            let primary_index = match schema.primary_index.len() {
                1 => schema.primary_index[0],
                0 => return Err(AerospikeSinkError::NoPrimaryKey.into()),
                _ => return Err(AerospikeSinkError::CompositePrimaryKey.into()),
            };
            match schema.fields[primary_index].typ {
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
                dozer_types::types::FieldType::Date) => warn!("Using a {typ} column as a primary key for Aerospike sink. This is only allowed because this type is converted to a String. Cast to another type explicitly to silence this warning."),

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
                    return Err(AerospikeSinkError::BinNameTooLong(field.name.to_owned()).into());
                }
            }
            let bin_names = schema
                .fields
                .iter()
                .map(|field| {
                    if field.name.len() <= AS_BIN_NAME_MAX_LEN as usize {
                        CString::new(field.name.clone()).map_err(AerospikeSinkError::NulError)
                    } else {
                        Err(AerospikeSinkError::BinNameTooLong(field.name.to_owned()))
                    }
                })
                .collect::<Result<_, _>>()?;

            let denormalizations = table
                .denormalize
                .iter()
                .map(|denorm| {
                    let columns: Vec<_> = denorm
                        .columns
                        .iter()
                        .cloned()
                        .map(|col| match col {
                            DenormColumn::Direct(name) => (name.clone(), name),
                            DenormColumn::Renamed {
                                source,
                                destination,
                            } => (source, destination),
                        })
                        .collect();
                    Denormalization::new(
                        &denorm.from_namespace,
                        &denorm.from_set,
                        schema.get_field_index(&denorm.key)?.0,
                        &columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let n_denormalization_cols = denormalizations
                .iter()
                .map(|denorm| denorm.columns.len() as u16)
                .sum();

            tables.push(AerospikeTable {
                namespace: CString::new(table.namespace.clone())?,
                set_name: CString::new(table.set_name.clone())?,
                primary_index,
                bin_names,
                denormalizations,
                n_denormalization_cols,
            });
        }
        let metadata_namespace = CString::new(self.config.metadata_namespace.clone())?;
        let metadata_set = CString::new(
            self.config
                .metadata_set
                .to_owned()
                .unwrap_or("__replication_metadata".to_owned()),
        )?;
        Ok(Box::new(AerospikeSink::new(
            client,
            tables,
            metadata_namespace,
            metadata_set,
            n_threads.into(),
        )))
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
    fn as_mut_ptr(&mut self) -> *mut as_record {
        self.0 as *mut as_record
    }

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
    snapshot_sender: Option<Sender<TableOperation>>,
    snapshotting_started_instant: HashMap<String, Instant>,
    snapshot_workers: Vec<JoinHandle<()>>,
    replication_worker: AerospikeSinkWorker,
    meta_sender: Sender<u64>,
    current_transaction: Option<u64>,
    n_snapshotting_threads: usize,
    catch_up_state: CatchingUpState,
    metadata_namespace: CString,
    metadata_set: CString,
    client: Arc<Client>,
}

type TxnId = u64;
struct MetadataWriter {
    client: Arc<Client>,
    key: NonNull<as_key>,
    record: NonNull<as_record>,
    receiver: Receiver<TxnId>,
}

// NonNull doesn't impl Send
unsafe impl Send for MetadataWriter {}

impl MetadataWriter {
    fn new(
        client: Arc<Client>,
        namespace: CString,
        set: CString,
        receiver: Receiver<TxnId>,
    ) -> Self {
        unsafe {
            let key = NonNull::new(as_key_new(
                namespace.as_ptr(),
                set.as_ptr(),
                META_KEY.as_ptr(),
            ))
            .unwrap();
            let record = NonNull::new(as_record_new(1)).unwrap();
            Self {
                client,
                key,
                record,
                receiver,
            }
        }
    }

    fn run(&mut self) {
        unsafe {
            while let Ok(txn_id) = self.receiver.recv() {
                as_record_set_int64(
                    self.record.as_ptr(),
                    META_TXN_ID_BIN.as_ptr(),
                    txn_id as i64,
                );
                if let Err(e) = self
                    .client
                    .upsert(self.key.as_ptr(), self.record.as_ptr(), None)
                {
                    error!("Error writing operation metadata: {e}")
                }
            }
        }
    }
}

impl Drop for MetadataWriter {
    fn drop(&mut self) {
        unsafe {
            as_record_destroy(self.record.as_ptr());
            as_key_destroy(self.key.as_ptr());
        }
    }
}

#[derive(Debug)]
struct Denormalization {
    namespace: CString,
    set: CString,
    key_field: usize,
    columns: Vec<(CString, CString)>,
    source_column_ptrs: Vec<*const c_char>,
}

// column ptrs
unsafe impl Send for Denormalization {}

impl Denormalization {
    fn new(
        namespace: &str,
        set: &str,
        key_field: usize,
        columns: &[(String, String)],
    ) -> Result<Self, AerospikeSinkError> {
        let namespace = CString::new(namespace)?;
        let set = CString::new(set)?;

        let columns = columns
            .iter()
            .map(|(src, dst)| Ok((CString::new(src.as_str())?, CString::new(dst.as_str())?)))
            .collect::<Result<Vec<_>, NulError>>()?;
        let mut source_column_ptrs: Vec<_> =
            columns.iter().map(|(src, _dst)| src.as_ptr()).collect();
        source_column_ptrs.push(null());
        Ok(Self {
            namespace,
            set,
            key_field,
            columns,
            source_column_ptrs,
        })
    }
}

impl Clone for Denormalization {
    fn clone(&self) -> Self {
        let columns = self.columns.clone();
        let mut source_column_ptrs: Vec<_> =
            columns.iter().map(|(src, _dst)| src.as_ptr()).collect();
        source_column_ptrs.push(null());
        Self {
            namespace: self.namespace.clone(),
            set: self.set.clone(),
            key_field: self.key_field,
            columns,
            source_column_ptrs,
        }
    }
}

#[derive(Debug, Clone)]
struct AerospikeTable {
    namespace: CString,
    set_name: CString,
    primary_index: usize,
    bin_names: Vec<CString>,
    denormalizations: Vec<Denormalization>,
    n_denormalization_cols: u16,
}

impl AerospikeSink {
    fn new(
        client: Client,
        tables: Vec<AerospikeTable>,
        metadata_namespace: CString,
        metadata_set: CString,
        n_threads: usize,
    ) -> Self {
        let client = Arc::new(client);

        let (meta_sender, meta_receiver) = unbounded();
        let worker_instance = AerospikeSinkWorker {
            client: client.clone(),
            tables,
        };

        let mut metadata_writer = MetadataWriter::new(
            client.clone(),
            metadata_namespace.clone(),
            metadata_set.clone(),
            meta_receiver,
        );
        std::thread::spawn(move || metadata_writer.run());

        Self {
            snapshot_sender: None,
            snapshotting_started_instant: Default::default(),
            snapshot_workers: vec![],
            replication_worker: worker_instance,
            meta_sender,
            current_transaction: None,
            n_snapshotting_threads: n_threads,
            catch_up_state: CatchingUpState::new(true),
            metadata_namespace,
            metadata_set,
            client,
        }
    }
}

fn convert_json(value: &JsonValue) -> Result<*mut as_bin_value, AerospikeSinkError> {
    unsafe {
        Ok(match value.destructure_ref() {
            // as_nil is a static, so we can't directly create a mutable pointer to it. We cast
            // through a const pointer instead. This location will never be written to,
            // because `free = false`
            DestructuredJsonRef::Null => addr_of!(as_nil) as *mut as_val as *mut as_bin_value,
            DestructuredJsonRef::Bool(value) => {
                check_alloc(as_boolean_new(value)) as *mut as_bin_value
            }
            DestructuredJsonRef::Number(value) => {
                if let Some(float) = value.to_f64() {
                    check_alloc(as_double_new(float)) as *mut as_bin_value
                } else if let Some(integer) = value.to_i64() {
                    check_alloc(as_integer_new(integer)) as *mut as_bin_value
                } else {
                    // If we can't represent as i64, we have a u64 that's larger than i64::MAX
                    return Err(AerospikeSinkError::IntegerOutOfRange(
                        value.to_u64().unwrap(),
                    ));
                }
            }
            DestructuredJsonRef::String(value) => {
                let bytes = check_alloc(as_bytes_new(value.len() as u32));
                as_bytes_set(bytes, 0, value.as_ptr(), value.len() as u32);
                (*bytes).type_ = as_bytes_type_e_AS_BYTES_STRING;
                bytes as *mut as_bin_value
            }
            DestructuredJsonRef::Array(value) => {
                let list = check_alloc(as_arraylist_new(value.len() as u32, value.len() as u32));
                for v in value.iter() {
                    let as_value = convert_json(v)?;
                    if as_arraylist_append(list, as_value as *mut as_val)
                        != as_status_e_AEROSPIKE_OK
                    {
                        as_arraylist_destroy(list);
                        return Err(AerospikeSinkError::CreateRecordError);
                    }
                }
                list as *mut as_bin_value
            }
            DestructuredJsonRef::Object(value) => {
                let map = check_alloc(as_orderedmap_new(value.len() as u32));
                struct Map(*mut as_orderedmap);
                impl Drop for Map {
                    fn drop(&mut self) {
                        unsafe {
                            as_orderedmap_destroy(self.0);
                        }
                    }
                }
                // Make sure the map is deallocated if we encounter any error...
                let _map_guard = Map(map);
                for (k, v) in value.iter() {
                    let as_value = convert_json(v)?;
                    let key = {
                        let bytes = check_alloc(as_bytes_new(k.len() as u32));
                        debug_assert!(as_bytes_set(bytes, 0, k.as_ptr(), k.len() as u32));
                        (*bytes).type_ = as_bytes_type_e_AS_BYTES_STRING;
                        bytes as *mut as_val
                    };
                    if as_orderedmap_set(map, key, as_value as *mut as_val) != 0 {
                        return Err(AerospikeSinkError::CreateRecordError);
                    };
                }
                // ...but don't deallocate if we succeed
                mem::forget(_map_guard);
                map as *mut as_bin_value
            }
        })
    }
}

#[derive(Clone, Debug)]
struct AerospikeSinkWorker {
    client: Arc<Client>,
    tables: Vec<AerospikeTable>,
}

#[derive(Debug)]
struct CatchingUpState {
    is_catching_up: bool,
    skipped_in_txn: usize,
    ops_in_txn: usize,
}

impl CatchingUpState {
    fn new(catching_up: bool) -> Self {
        Self {
            is_catching_up: catching_up,
            skipped_in_txn: 0,
            ops_in_txn: 0,
        }
    }

    fn on_op(&mut self, skipped: bool) {
        self.ops_in_txn += 1;
        if skipped {
            self.skipped_in_txn += 1;
        }
    }

    fn commit(&mut self) {
        if self.ops_in_txn > 0 && self.skipped_in_txn == 0 {
            self.is_catching_up = false;
        }
        self.ops_in_txn = 0;
        self.skipped_in_txn = 0;
    }
}

impl AerospikeSinkWorker {
    fn snapshot(&mut self, receiver: Receiver<TableOperation>) {
        while let Ok(op) = receiver.recv() {
            if let Err(e) = self.process(op, false) {
                error!("Error processing operation: {}", e);
            }
        }
    }

    #[inline]
    fn set_str_key(
        &self,
        key: *mut as_key,
        namespace: &CStr,
        set: &CStr,
        mut string: String,
        allocated_strings: &mut Vec<String>,
    ) {
        unsafe {
            let bytes = as_bytes_new_wrap(string.as_mut_ptr(), string.len() as u32, false);
            (*bytes).type_ = as_bytes_type_e_AS_BYTES_STRING;
            allocated_strings.push(string);
            as_key_init_value(
                key,
                namespace.as_ptr(),
                set.as_ptr(),
                bytes as *const _ as *const as_key_value,
            );
        }
    }

    unsafe fn init_key(
        &self,
        key: *mut as_key,
        namespace: &CStr,
        set: &CStr,
        key_field: &Field,
        allocated_strings: &mut Vec<String>,
    ) -> Result<(), AerospikeSinkError> {
        unsafe {
            match key_field {
                Field::UInt(v) => {
                    as_key_init_int64(key, namespace.as_ptr(), set.as_ptr(), *v as i64);
                }
                Field::Int(v) => {
                    as_key_init_int64(key, namespace.as_ptr(), set.as_ptr(), *v);
                }
                Field::U128(v) => {
                    self.set_str_key(key, namespace, set, v.to_string(), allocated_strings)
                }
                Field::I128(v) => {
                    self.set_str_key(key, namespace, set, v.to_string(), allocated_strings)
                }
                Field::Decimal(v) => {
                    self.set_str_key(key, namespace, set, v.to_string(), allocated_strings)
                }
                // For keys, we need to allocate a new CString, because there is no
                // API to set a key to a string that's not null-terminated. For bin
                // values, we can. XXX: possible point for optimization
                Field::Text(string) | Field::String(string) => {
                    // Casting to mut is safe. The pointer only needs to be mut so it
                    // can be deallocated if the `free` parameter is true
                    let bytes =
                        as_bytes_new_wrap(string.as_ptr() as *mut u8, string.len() as u32, false);
                    (*bytes).type_ = as_bytes_type_e_AS_BYTES_STRING;
                    as_key_init_value(
                        key,
                        namespace.as_ptr(),
                        set.as_ptr(),
                        bytes as *const _ as *const as_key_value,
                    );
                }
                Field::Binary(v) => {
                    as_key_init_rawp(
                        key,
                        namespace.as_ptr(),
                        set.as_ptr(),
                        v.as_ptr(),
                        v.len() as u32,
                        false,
                    );
                }

                Field::Timestamp(v) => self.set_str_key(
                    key,
                    namespace,
                    set,
                    // Use a delayed formatting to RFC3339 so we don't have to allocate an
                    // intermediate rust String
                    v.to_rfc3339(),
                    allocated_strings,
                ),
                // Date's display implementation is RFC3339 compatible
                Field::Date(v) => {
                    self.set_str_key(key, namespace, set, v.to_string(), allocated_strings)
                }
                // We can ignore the time unit, as we always output a
                // full-resolution duration
                Field::Duration(DozerDuration(duration, _)) => self.set_str_key(
                    key,
                    namespace,
                    set,
                    format!("PT{},{:09}S", duration.as_secs(), duration.subsec_nanos()),
                    allocated_strings,
                ),
                Field::Null => unreachable!("Primary key cannot be null"),
                Field::Boolean(_) | Field::Json(_) | Field::Point(_) | Field::Float(_) => {
                    unreachable!("Unsupported primary key type. If this is reached, it means this record does not conform to the schema.")
                }
            };
        }
        Ok(())
    }

    unsafe fn rec_set_str(
        record: *mut as_record,
        name: *const c_char,
        string: String,
        allocated_strings: &mut Vec<String>,
    ) {
        Self::rec_set_bytes(
            record,
            name,
            string.as_bytes(),
            as_bytes_type_e_AS_BYTES_STRING,
        );
        allocated_strings.push(string);
    }

    unsafe fn rec_set_bytes(
        record: *mut as_record,
        name: *const c_char,
        bytes: &[u8],
        type_: as_bytes_type,
    ) {
        let ptr = bytes.as_ptr();
        let len = bytes.len();
        as_record_set_raw_typep(record, name, ptr, len as u32, type_, false);
    }

    unsafe fn init_record(
        &self,
        record: *mut as_record,
        dozer_record: &Record,
        bin_names: &[CString],
        n_extra_cols: u16,
        op_id: Option<OpIdentifier>,
        allocated_strings: &mut Vec<String>,
    ) -> Result<(), AerospikeSinkError> {
        as_record_init(
            record,
            dozer_record.values.len() as u16 + n_extra_cols /* denorm */ + 2, /* tx_id and seq */
        );
        for (def, field) in bin_names.iter().zip(&dozer_record.values) {
            let name = def.as_ptr();
            match field {
                Field::UInt(v) => {
                    as_record_set_int64(record, name, *v as i64);
                }
                Field::U128(v) => {
                    Self::rec_set_str(record, name, v.to_string(), allocated_strings);
                }
                Field::Int(v) => {
                    as_record_set_int64(record, name, *v);
                }
                Field::I128(v) => {
                    Self::rec_set_str(record, name, v.to_string(), allocated_strings);
                }
                Field::Float(OrderedFloat(v)) => {
                    as_record_set_double(record, name, *v);
                }
                Field::Boolean(v) => {
                    as_record_set_bool(record, name, *v);
                }
                Field::String(v) | Field::Text(v) => {
                    as_record_set_raw_typep(
                        record,
                        name,
                        v.as_ptr(),
                        v.len() as u32,
                        as_bytes_type_e_AS_BYTES_STRING,
                        false,
                    );
                }
                Field::Binary(v) => {
                    as_record_set_rawp(record, name, v.as_ptr(), v.len() as u32, false);
                }
                Field::Decimal(v) => {
                    Self::rec_set_str(record, name, v.to_string(), allocated_strings);
                }
                Field::Timestamp(v) => {
                    Self::rec_set_str(record, name, v.to_rfc3339(), allocated_strings);
                }
                // Date's display implementation is RFC3339 compatible
                Field::Date(v) => {
                    Self::rec_set_str(record, name, v.to_string(), allocated_strings);
                }
                Field::Duration(DozerDuration(duration, _)) => {
                    Self::rec_set_str(
                        record,
                        name,
                        format!("PT{},{:09}S", duration.as_secs(), duration.subsec_nanos()),
                        allocated_strings,
                    );
                }
                Field::Null => {
                    as_record_set_nil(record, name);
                }
                // XXX: Geojson points have to have coordinates <90. Dozer points can
                // be arbitrary locations.
                Field::Point(DozerPoint(Point(Coord { x, y }))) => {
                    // Using our string-as-bytes trick does not work, as BYTES_GEOJSON is not
                    // a plain string format. Instead, we just make sure we include a nul-byte
                    // in our regular string, as that is easiest to integration with the other
                    // string allocations.
                    let string = format!(
                        r#"{{"type": "Point", "coordinates": [{}, {}]}}{}"#,
                        x.0, y.0, '\0'
                    );
                    as_record_set_geojson_strp(record, name, string.as_ptr().cast(), false);
                    allocated_strings.push(string);
                }
                Field::Json(v) => {
                    let value = convert_json(v)?;
                    as_record_set(record, name, value);
                }
            }
        }
        if let Some(op_id) = op_id {
            let OpIdentifier { txid, seq_in_tx } = op_id;
            as_record_set_int64(record, constants::TXN_ID_BIN.as_ptr(), txid as i64);
            as_record_set_int64(record, constants::TXN_SEQ_BIN.as_ptr(), seq_in_tx as i64);
        }
        Ok(())
    }

    unsafe fn set_operation_str(
        ops: *mut as_operations,
        name: *const c_char,
        mut string: String,
        allocated_strings: &mut Vec<String>,
    ) {
        let ptr = string.as_mut_ptr();
        let len = string.len();
        allocated_strings.push(string);
        // Unfortunately we need to do an allocation here for the bytes container.
        // This is because as_operations does not allow setting a bytes type in
        // its operations api. TODO: Add a raw_typep api like `as_record_set_raw_typep`
        // for as_operations
        let bytes = as_bytes_new_wrap(ptr, len as u32, false);
        (*bytes).type_ = as_bytes_type_e_AS_BYTES_STRING;
        as_operations_add_write(ops, name, bytes as *mut as_bin_value);
    }

    unsafe fn init_ops(
        &self,
        ops: *mut as_operations,
        dozer_record: &Record,
        bin_names: &[CString],
        allocated_strings: &mut Vec<String>,
    ) -> Result<(), AerospikeSinkError> {
        for (def, field) in bin_names.iter().zip(&dozer_record.values) {
            let name = def.as_ptr();
            // This is almost the same as the implementation for keys,
            // the key difference being that we don't have to allocate a new
            // string, because we can use `as_record_set_raw_typep` to set
            // rust strings directly without intermediate allocations
            // TODO: Unify the implementations
            match field {
                Field::UInt(v) => {
                    as_operations_add_write_int64(ops, name, *v as i64);
                }
                Field::U128(v) => {
                    Self::set_operation_str(ops, name, v.to_string(), allocated_strings);
                }
                Field::Int(v) => {
                    as_operations_add_write_int64(ops, name, *v);
                }
                Field::I128(v) => {
                    Self::set_operation_str(ops, name, v.to_string(), allocated_strings);
                }
                Field::Float(v) => {
                    as_operations_add_write_double(ops, name, v.0);
                }
                Field::Boolean(v) => {
                    as_operations_add_write_bool(ops, name, *v);
                }
                Field::String(string) | Field::Text(string) => {
                    let ptr = string.as_ptr();
                    let len = string.len();
                    // Casting to *mut is safe because aerospike won't write
                    // to it if `free` is false
                    let bytes = as_bytes_new_wrap(ptr as *mut u8, len as u32, false);
                    (*bytes).type_ = as_bytes_type_e_AS_BYTES_STRING;
                    as_operations_add_write(ops, name, bytes as *mut as_bin_value);
                }
                Field::Binary(v) => {
                    as_operations_add_write_rawp(ops, name, v.as_ptr(), v.len() as u32, false);
                }
                Field::Decimal(v) => {
                    Self::set_operation_str(ops, name, v.to_string(), allocated_strings);
                }
                Field::Timestamp(v) => {
                    Self::set_operation_str(ops, name, v.to_rfc3339(), allocated_strings);
                }
                // Date's display implementation is RFC3339 compatible
                Field::Date(v) => {
                    Self::set_operation_str(ops, name, v.to_string(), allocated_strings);
                }
                Field::Duration(DozerDuration(duration, _)) => {
                    Self::set_operation_str(
                        ops,
                        name,
                        format!("PT{},{:09}S", duration.as_secs(), duration.subsec_nanos()),
                        allocated_strings,
                    );
                }
                Field::Null => {
                    // as_bin_value is a union, with nil being an as_val. It is therefore
                    // valid to just cast a pointer to the as_nil constant (of type as_val),
                    // as its location is static
                    as_operations_add_write(ops, name, addr_of!(as_nil) as *mut as_bin_value);
                }
                Field::Point(DozerPoint(Point(Coord { x, y }))) => {
                    // Using our string-as-bytes trick does not work, as BYTES_GEOJSON is not
                    // a plain string format. Instead, we just make sure we include a nul-byte
                    // in our regular string, as that is easiest to integration with the other
                    // string allocations being `String` and not `CString`. We know we whttps://docs.oracle.com/en/database/oracle/oracle-database/19/ladbi/running-oracle-universal-installer-to-install-oracle-database.html#GUID-DD4800E9-C651-4B08-A6AC-E5ECCC6512B9on't
                    // have any intermediate nul-bytes, as we control the string
                    let string = format!(
                        r#"{{"type": "Point", "coordinates": [{}, {}]}}{}"#,
                        x.0, y.0, '\0'
                    );
                    as_operations_add_write_geojson_strp(ops, name, string.as_ptr().cast(), false);
                    allocated_strings.push(string);
                }
                Field::Json(v) => {
                    as_operations_add_write(ops, name, convert_json(v)?);
                }
            }
        }
        Ok(())
    }
    fn process(
        &mut self,
        op: TableOperation,
        filter_by_opid: bool,
    ) -> Result<(), AerospikeSinkError> {
        let table = &self.tables[op.port as usize];
        if !table.denormalizations.is_empty() {
            if let Operation::BatchInsert { new } = op.op {
                for rec in new.into_iter() {
                    self.process(
                        TableOperation {
                            op: Operation::Insert { new: rec },
                            id: op.id,
                            port: op.port,
                        },
                        filter_by_opid,
                    )?;
                }
                return Ok(());
            }
        }
        // XXX: We know from the schema how many strings we have to allocate,
        // so we could optimize this to allocate the correct amount ahead
        // of time. Furthermore, we also know (an upper bound of) the total size of the strings we
        // have to allocate, so we could just allocate one large Vec<u8>, and
        // use that for all string allocations, like an arena
        let mut allocated_strings = Vec::new();
        let id = op.id;

        let filter = id.filter(|_| filter_by_opid).map(|id| unsafe {
            NonNull::new(as_exp_build!(as_exp_or(
                as_exp_cmp_lt(
                    as_exp_bin_int(constants::TXN_ID_BIN.as_ptr()),
                    as_exp_int(id.txid as i64)
                ),
                as_exp_and(
                    as_exp_cmp_eq(
                        as_exp_bin_int(constants::TXN_ID_BIN.as_ptr()),
                        as_exp_int(id.txid as i64)
                    ),
                    as_exp_cmp_lt(
                        as_exp_bin_int(constants::TXN_SEQ_BIN.as_ptr()),
                        as_exp_int(id.seq_in_tx as i64)
                    )
                )
            )))
            .unwrap()
        });
        match op.op {
            Operation::Insert { new } => {
                // We create the key and record on the stack, because we can
                // and it saves an allocation. These structs are self-referential
                // on the C side (for value-types), so make sure not to move them.
                // This prevents us from creating a wrapper type responsible for cleaning
                // up, as that would move. We could probably make some kind of smart wrapper
                // using PhantomPinned and `pin!()`
                let mut key = MaybeUninit::uninit();
                let mut _record = MaybeUninit::uninit();

                unsafe {
                    self.init_key(
                        key.as_mut_ptr(),
                        &table.namespace,
                        &table.set_name,
                        &new.values[table.primary_index],
                        &mut allocated_strings,
                    )?;
                    let k = Key(key.assume_init_mut());
                    self.init_record(
                        _record.as_mut_ptr(),
                        &new,
                        &table.bin_names,
                        table.n_denormalization_cols,
                        id,
                        &mut allocated_strings,
                    )?;
                    let mut record = AsRecord(_record.assume_init_mut());
                    for Denormalization {
                        key_field,
                        source_column_ptrs,
                        namespace,
                        set,
                        columns,
                    } in &table.denormalizations
                    {
                        let mut _key = MaybeUninit::uninit();
                        self.init_key(
                            _key.as_mut_ptr(),
                            namespace,
                            set,
                            &new.values[*key_field],
                            &mut allocated_strings,
                        )?;
                        let key = Key(_key.assume_init_mut());
                        let mut _rec = MaybeUninit::uninit();
                        as_record_init(_rec.as_mut_ptr(), columns.len() as u16);
                        let mut denorm_rec = AsRecord(_rec.assume_init_mut());
                        loop {
                            #[allow(non_upper_case_globals)]
                            match self.client.select(
                                key.as_ptr(),
                                source_column_ptrs,
                                &mut denorm_rec.as_mut_ptr(),
                            ) {
                                Ok(()) => break,
                                // If the record is not found, wait and try again,
                                // we are probably behind the task responsible for writing it
                                Err(AerospikeError {
                                    code: as_status_e_AEROSPIKE_ERR_RECORD_NOT_FOUND,
                                    message: _,
                                }) => std::thread::sleep(Duration::from_millis(100)),
                                Err(e) => return Err(e.into()),
                            }
                        }
                        // The column_ptrs array needs to end with a null ptr, so use
                        // `columns` for the bound instead
                        for (src, dst) in columns {
                            let val = as_record_get(denorm_rec.as_mut_ptr(), src.as_ptr());

                            // Increment ref count, so we can destroy the denorm record
                            // without dropping the bin values
                            as_val_val_reserve(val as *mut as_val);
                            as_record_set(record.as_mut_ptr(), dst.as_ptr(), val);
                        }
                        as_record_destroy(denorm_rec.as_mut_ptr());
                    }
                    self.client
                        .insert(k.as_ptr(), record.as_mut_ptr(), filter)?;
                }
            }
            Operation::Delete { old } => {
                let mut key = MaybeUninit::uninit();
                unsafe {
                    self.init_key(
                        key.as_mut_ptr(),
                        &table.namespace,
                        &table.set_name,
                        &old.values[table.primary_index],
                        &mut allocated_strings,
                    )?;
                    let k = Key(key.assume_init_mut());
                    self.client.delete(k.as_ptr(), filter)?;
                }
            }
            Operation::Update { old, new } => {
                {
                    let old_pk = &old.values[table.primary_index];
                    let new_pk = &new.values[table.primary_index];
                    if old_pk != new_pk {
                        return Err(AerospikeSinkError::PrimaryKeyChanged {
                            old: old_pk.clone(),
                            new: new_pk.clone(),
                        });
                    }
                }
                let mut key = MaybeUninit::uninit();
                let mut record = MaybeUninit::uninit();
                unsafe {
                    self.init_key(
                        key.as_mut_ptr(),
                        &table.namespace,
                        &table.set_name,
                        &old.values[table.primary_index],
                        &mut allocated_strings,
                    )?;
                    let k = Key(key.assume_init_mut());
                    self.init_record(
                        record.as_mut_ptr(),
                        &new,
                        &table.bin_names,
                        0,
                        id,
                        &mut allocated_strings,
                    )?;
                    let mut r = AsRecord(record.assume_init_mut());
                    self.client.update(k.as_ptr(), r.as_mut_ptr(), filter)?;
                }
            }
            Operation::BatchInsert { new } => {
                // Create an as_batch_write_record for each key
                // Create an as_operations for each bin and assign them to the
                // as_batch_write_record
                let mut batch = unsafe {
                    let mut batch = MaybeUninit::uninit();
                    as_batch_records_init(batch.as_mut_ptr(), new.len() as u32);
                    Batch(batch.assume_init())
                };
                // Wrapper type here, so `as_operations_destroy` is called, even
                // when an error occurs
                let mut operations = Operations::new(new.len());
                for dozer_record in new.iter() {
                    unsafe {
                        let record = as_batch_write_reserve(batch.as_ptr());
                        let ops = operations.next(dozer_record.values.len());
                        if ops.is_null() {
                            return Err(AerospikeSinkError::CreateRecordError);
                        }
                        self.init_ops(ops, dozer_record, &table.bin_names, &mut allocated_strings)?;
                        (*record).ops = ops;
                        self.init_key(
                            &mut (*record).key as *mut as_key,
                            &table.namespace,
                            &table.set_name,
                            &dozer_record.values[table.primary_index],
                            &mut allocated_strings,
                        )?;
                    }
                }
                unsafe {
                    self.client.insert_batch(batch.as_ptr())?;
                }
            }
        }
        Ok(())
    }
}

struct Operations(Vec<MaybeUninit<as_operations>>);
impl Operations {
    fn new(size: usize) -> Self {
        Self(Vec::with_capacity(size))
    }

    /// SAFETY:
    /// May only be called at most `size` times. If called more often, the previously
    /// returned pointers are invalidated
    unsafe fn next(&mut self, n_bins: usize) -> *mut as_operations {
        debug_assert!(self.0.len() < self.0.capacity()); // Check that we don't reallocate
        self.0.push(MaybeUninit::uninit());
        let ptr = self.0.last_mut().unwrap().as_mut_ptr();
        let init = unsafe { as_operations_init(ptr, n_bins.try_into().unwrap()) };
        if init.is_null() {
            self.0.pop();
        }
        init
    }
}

impl Drop for Operations {
    fn drop(&mut self) {
        unsafe {
            for ops in self.0.iter_mut() {
                as_operations_destroy(ops.as_mut_ptr());
            }
        }
    }
}

#[repr(transparent)]
struct Batch(as_batch_records);
impl Batch {
    fn as_ptr(&mut self) -> *mut as_batch_records {
        &mut self.0 as *mut _
    }
}

impl Drop for Batch {
    fn drop(&mut self) {
        unsafe {
            as_batch_records_destroy(&mut self.0 as *mut as_batch_records);
        }
    }
}

#[inline(always)]
unsafe fn as_vector_reserve(vector: *mut as_vector) -> *mut c_void {
    if (*vector).size >= (*vector).capacity {
        as_vector_increase_capacity(vector);
    }
    let item = (*vector)
        .list
        .byte_add((*vector).size as usize * (*vector).item_size as usize);
    (item as *mut u8).write_bytes(0, (*vector).item_size as usize);
    (*vector).size += 1;
    item
}

#[inline(always)]
unsafe fn as_batch_write_reserve(records: *mut as_batch_records) -> *mut as_batch_write_record {
    let r = as_vector_reserve(&mut (*records).list as *mut as_vector) as *mut as_batch_write_record;
    (*r).type_ = AS_BATCH_WRITE as u8;
    (*r).has_write = true;
    r
}

#[inline(always)]
unsafe fn as_batch_records_init(records: *mut as_batch_records, capacity: u32) {
    as_vector_init(
        // Can't go through a reference, because this field might not be initialized
        addr_of_mut!((*records).list),
        mem::size_of::<as_batch_record>() as u32,
        capacity,
    );
}

impl Sink for AerospikeSink {
    fn commit(&mut self, _epoch_details: &dozer_core::epoch::Epoch) -> Result<(), BoxedError> {
        if let Some(transaction) = self.current_transaction.take() {
            self.meta_sender.send(transaction)?;
        }
        self.catch_up_state.commit();
        Ok(())
    }

    fn process(&mut self, op: TableOperation) -> Result<(), BoxedError> {
        debug_assert_eq!(op.port, DEFAULT_PORT_HANDLE);
        if let Some(snapshot_sender) = &mut self.snapshot_sender {
            snapshot_sender.send(op)?;
        } else {
            // Set current transaction before any error can be thrown, so we don't
            // get stuck in an error loop if this error gets ignored by the caller
            self.current_transaction = op.id.map(|id| id.txid);

            let filter_by_opid = self.catch_up_state.is_catching_up;

            #[allow(non_upper_case_globals)]
            match self.replication_worker.process(op, filter_by_opid) {
                Err(AerospikeSinkError::Aerospike(AerospikeError {
                    code:
                        as_status_e_AEROSPIKE_FILTERED_OUT | as_status_e_AEROSPIKE_ERR_RECORD_NOT_FOUND,
                    message: _,
                })) if filter_by_opid => {
                    self.catch_up_state.on_op(true);
                }
                Err(e) => return Err(Box::new(e)),
                Ok(_) => {
                    self.catch_up_state.on_op(false);
                }
            }
        }
        Ok(())
    }

    fn persist(
        &mut self,
        _epoch: &dozer_core::epoch::Epoch,
        _queue: &dozer_log::storage::Queue,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_started(
        &mut self,
        connection_name: String,
    ) -> Result<(), BoxedError> {
        let n_threads = self.n_snapshotting_threads;
        self.snapshotting_started_instant
            .insert(connection_name, Instant::now());
        let mut workers = Vec::with_capacity(n_threads);
        let (sender, receiver) = bounded(n_threads);

        for _ in 0..n_threads {
            let mut worker = self.replication_worker.clone();
            let receiver = receiver.clone();
            workers.push(std::thread::spawn(move || worker.snapshot(receiver)));
        }
        self.snapshot_workers = workers;
        self.snapshot_sender = Some(sender);
        self.catch_up_state = CatchingUpState::new(false);

        Ok(())
    }

    fn on_source_snapshotting_done(
        &mut self,
        connection_name: String,
        id: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        if let Some(started_instant) = self.snapshotting_started_instant.remove(&connection_name) {
            info!(
                "Snapshotting for connection {} took {:?}",
                connection_name,
                started_instant.elapsed()
            );
        } else {
            warn!(
                "Snapshotting for connection {} took unknown time",
                connection_name
            );
        }
        // Drop the old sender, then wait for all threads to finish
        let _ = std::mem::take(&mut self.snapshot_sender);
        for handle in self.snapshot_workers.drain(..) {
            handle.join().unwrap();
        }
        if let Some(id) = id {
            // Store the transaction id, so we can resume from this point
            self.meta_sender.send(id.txid)?;
        }
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
            let txid =
                as_record_get_int64(record.as_ptr(), constants::META_TXN_ID_BIN.as_ptr(), -1);
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
}

#[cfg(test)]
mod tests {

    use dozer_log::tokio;
    use std::time::Duration;

    use dozer_types::{
        chrono::{DateTime, NaiveDate},
        models::sink::AerospikeSinkTable,
        ordered_float::OrderedFloat,
        rust_decimal::Decimal,
        types::FieldDefinition,
    };

    use super::*;

    fn f(name: &str, typ: FieldType) -> FieldDefinition {
        FieldDefinition {
            name: name.to_owned(),
            typ,
            nullable: false,
            source: dozer_types::types::SourceDefinition::Dynamic,
        }
    }

    const N_RECORDS: usize = 1000;
    const BATCH_SIZE: usize = 1000;

    #[tokio::test]
    #[ignore]
    async fn test_inserts() {
        let mut sink = sink("inserts").await;
        for i in 0..N_RECORDS {
            sink.process(TableOperation::without_id(
                Operation::Insert {
                    new: record(i as u64),
                },
                DEFAULT_PORT_HANDLE,
            ))
            .unwrap();
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_inserts_batch() {
        let mut batches = Vec::with_capacity(N_RECORDS / BATCH_SIZE);
        for i in 0..N_RECORDS / BATCH_SIZE {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            for j in (i * BATCH_SIZE)..((i + 1) * BATCH_SIZE) {
                batch.push(record(j as u64));
            }
            batches.push(batch);
        }
        let mut sink = sink("inserts_batch").await;
        for batch in batches {
            sink.process(TableOperation::without_id(
                Operation::BatchInsert { new: batch },
                DEFAULT_PORT_HANDLE,
            ))
            .unwrap()
        }
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
                n_threads: Some(1.try_into().unwrap()),
                tables: vec![AerospikeSinkTable {
                    source_table_name: "test".into(),
                    namespace: "test".into(),
                    set_name: set.to_owned(),
                    denormalize: vec![],
                }],
                metadata_namespace: "test".into(),
                metadata_set: None,
            },
        );
        factory
            .build([(DEFAULT_PORT_HANDLE, schema)].into())
            .await
            .unwrap()
    }

    fn record(i: u64) -> Record {
        Record::new(vec![
            Field::UInt(i),
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
            Field::Point(DozerPoint(Point::new(
                OrderedFloat((i % 90) as f64),
                OrderedFloat((i % 90) as f64),
            ))),
            Field::Duration(DozerDuration(
                Duration::from_secs(i),
                dozer_types::types::TimeUnit::Seconds,
            )),
            Field::Null,
            Field::Json(dozer_types::json_types::json!({
                i.to_string(): i,
                i.to_string(): i as f64,
                "array": vec![i; 5],
                "object": {
                    "haha": i
                }
            })),
        ])
    }
}
