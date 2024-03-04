use std::{
    alloc::{handle_alloc_error, Layout},
    ffi::{c_char, c_void, CStr, CString, NulError},
    mem::MaybeUninit,
    ptr::{addr_of, NonNull},
    slice,
};

use itertools::Itertools;

use aerospike_client_sys::*;
use dozer_types::{
    chrono::{DateTime, NaiveDate},
    geo::{Coord, Point},
    json_types::{DestructuredJsonRef, JsonValue},
    ordered_float::OrderedFloat,
    rust_decimal::prelude::*,
    thiserror,
    types::{DozerDuration, DozerPoint, Field, Record, Schema},
};

use crate::{denorm_dag::Error, AerospikeSinkError};

#[derive(Debug)]
pub struct BinNames {
    storage: Vec<CString>,
    _ptrs: Vec<*mut i8>,
}

unsafe impl Send for BinNames {}

impl Clone for BinNames {
    fn clone(&self) -> Self {
        let storage = self.storage.clone();
        let ptrs = Self::make_ptrs(&storage);
        Self {
            storage,
            _ptrs: ptrs,
        }
    }
}

impl BinNames {
    fn make_ptrs(storage: &[CString]) -> Vec<*mut i8> {
        storage
            .iter()
            .map(|name| name.as_ptr() as *mut i8)
            .collect()
    }

    pub(crate) fn _len(&self) -> usize {
        self.storage.len()
    }

    pub(crate) unsafe fn _ptrs(&mut self) -> *mut *mut i8 {
        self._ptrs.as_mut_ptr()
    }

    pub(crate) fn names(&self) -> &[CString] {
        &self.storage
    }

    pub(crate) fn new<I: IntoIterator<Item = impl AsRef<str>>>(names: I) -> Result<Self, NulError> {
        let storage: Vec<CString> = names
            .into_iter()
            .map(|name| CString::new(name.as_ref()))
            .collect::<Result<_, _>>()?;
        let ptrs = Self::make_ptrs(&storage);
        Ok(Self {
            storage,
            _ptrs: ptrs,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub struct AerospikeError {
    pub(crate) code: i32,
    pub(crate) message: String,
}

impl AerospikeError {
    pub(crate) fn from_code(value: as_status) -> Self {
        let message = unsafe { as_error_string(value) };

        let message = unsafe { CStr::from_ptr(message) };
        // The message is ASCII (I think?), so this should not fail
        Self {
            code: value,
            message: message.to_str().unwrap().to_owned(),
        }
    }
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

impl std::fmt::Display for AerospikeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.code, self.message)
    }
}

// Client should never be `Clone`, because of the custom Drop impl
#[derive(Debug)]
pub struct Client {
    inner: NonNull<aerospike>,
}

// The aerospike client API is thread-safe.
unsafe impl Send for Client {}
unsafe impl Sync for Client {}

#[inline(always)]
pub(crate) unsafe fn check_alloc<T>(ptr: *mut T) -> *mut T {
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
    pub fn new(hosts: &CStr) -> Result<Self, AerospikeError> {
        let mut config = unsafe {
            let mut config = MaybeUninit::uninit();
            as_config_init(config.as_mut_ptr());
            config.assume_init()
        };
        config.policies.batch.base.total_timeout = 10000;
        config.policies.write.key = as_policy_key_e_AS_POLICY_KEY_SEND;
        config.policies.batch_write.key = as_policy_key_e_AS_POLICY_KEY_SEND;
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
            }
            let this = Self {
                inner: NonNull::new_unchecked(inner),
            };
            this.connect()?;
            this
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

    pub(crate) unsafe fn _insert(
        &self,
        key: *const as_key,
        new: *mut as_record,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        let mut policy = self.inner.as_ref().config.policies.write;
        policy.exists = as_policy_exists_e_AS_POLICY_EXISTS_CREATE;
        self.put(key, new, policy, filter)
    }

    pub(crate) unsafe fn _update(
        &self,
        key: *const as_key,
        new: *mut as_record,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        let mut policy = self.inner.as_ref().config.policies.write;
        policy.exists = as_policy_exists_e_AS_POLICY_EXISTS_UPDATE;
        self.put(key, new, policy, filter)
    }

    pub(crate) unsafe fn upsert(
        &self,
        key: *const as_key,
        new: *mut as_record,
        filter: Option<NonNull<as_exp>>,
    ) -> Result<(), AerospikeError> {
        let mut policy = self.inner.as_ref().config.policies.write;
        policy.exists = as_policy_exists_e_AS_POLICY_EXISTS_CREATE_OR_REPLACE;
        self.put(key, new, policy, filter)
    }

    pub(crate) unsafe fn _delete(
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

    pub(crate) unsafe fn write_batch(
        &self,
        batch: *mut as_batch_records,
    ) -> Result<(), AerospikeError> {
        as_try(|err| aerospike_batch_write(self.inner.as_ptr(), err, std::ptr::null(), batch))
    }

    pub(crate) unsafe fn _select(
        &self,
        key: *const as_key,
        bins: &[*const c_char],
        record: &mut *mut as_record,
    ) -> Result<(), AerospikeError> {
        as_try(|err| {
            aerospike_key_select(
                self.inner.as_ptr(),
                err,
                std::ptr::null(),
                key,
                // This won't write to the mut ptr
                bins.as_ptr() as *mut *const c_char,
                record as *mut *mut as_record,
            )
        })
    }
    pub(crate) unsafe fn get(
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

    pub(crate) unsafe fn batch_get(
        &self,
        batch: *mut as_batch_records,
    ) -> Result<(), AerospikeError> {
        dbg!("Batch get {} records", (*batch).list.size);
        as_try(|err| aerospike_batch_read(self.inner.as_ptr(), err, std::ptr::null(), batch))
    }

    /// # Safety
    /// The caller is responsible for cleaning up the response
    ///
    /// This function sends a raw info request to the aerospike server
    pub unsafe fn info(
        &self,
        request: &CStr,
        response: &mut *mut i8,
    ) -> Result<(), AerospikeError> {
        as_try(|err| {
            aerospike_info_any(
                self.inner.as_ptr(),
                err,
                std::ptr::null(),
                request.as_ptr(),
                response as *mut *mut i8,
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

pub(crate) fn convert_json(value: &JsonValue) -> Result<*mut as_bin_value, AerospikeSinkError> {
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
                std::mem::forget(_map_guard);
                map as *mut as_bin_value
            }
        })
    }
}

#[inline]
fn set_str_key(
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

pub(crate) unsafe fn init_key(
    key: *mut as_key,
    namespace: &CStr,
    set: &CStr,
    key_fields: &[Field],
    allocated_strings: &mut Vec<String>,
) -> Result<(), AerospikeSinkError> {
    assert!(!key_fields.is_empty());
    // Fast option
    if key_fields.len() == 1 {
        return init_key_single(key, namespace, set, &key_fields[0], allocated_strings);
    }

    let key_string = key_fields.iter().join("_");
    set_str_key(key, namespace, set, key_string, allocated_strings);

    Ok(())
}

unsafe fn init_key_single(
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
            Field::U128(v) => set_str_key(key, namespace, set, v.to_string(), allocated_strings),
            Field::I128(v) => set_str_key(key, namespace, set, v.to_string(), allocated_strings),
            Field::Decimal(v) => set_str_key(key, namespace, set, v.to_string(), allocated_strings),
            // For keys, we need to allocate a new CString, because there is no
            // API to set a key to a string that's not null-terminated. For bin
            // values, we can. XXX: possible point for optimization
            Field::Text(string) | Field::String(string) => {
                set_str_key(key, namespace, set, string.clone(), allocated_strings);
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

            Field::Timestamp(v) => {
                set_str_key(key, namespace, set, v.to_rfc3339(), allocated_strings)
            }
            // Date's display implementation is RFC3339 compatible
            Field::Date(v) => set_str_key(key, namespace, set, v.to_string(), allocated_strings),
            // We can ignore the time unit, as we always output a
            // full-resolution duration
            Field::Duration(DozerDuration(duration, _)) => set_str_key(
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
    rec_set_bytes(
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

#[allow(unused)]
pub(crate) unsafe fn init_record(
    record: *mut as_record,
    dozer_record: &Record,
    bin_names: &[CString],
    n_extra_cols: u16,
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
                rec_set_str(record, name, v.to_string(), allocated_strings);
            }
            Field::Int(v) => {
                as_record_set_int64(record, name, *v);
            }
            Field::I128(v) => {
                rec_set_str(record, name, v.to_string(), allocated_strings);
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
                rec_set_str(record, name, v.to_string(), allocated_strings);
            }
            Field::Timestamp(v) => {
                rec_set_str(record, name, v.to_rfc3339(), allocated_strings);
            }
            // Date's display implementation is RFC3339 compatible
            Field::Date(v) => {
                rec_set_str(record, name, v.to_string(), allocated_strings);
            }
            Field::Duration(DozerDuration(duration, _)) => {
                rec_set_str(
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

pub(crate) unsafe fn init_batch_write_operations(
    ops: *mut as_operations,
    dozer_record: &[Field],
    bin_names: &[CString],
    allocated_strings: &mut Vec<String>,
) -> Result<(), AerospikeSinkError> {
    for (def, field) in bin_names.iter().zip(dozer_record) {
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
                set_operation_str(ops, name, v.to_string(), allocated_strings);
            }
            Field::Int(v) => {
                as_operations_add_write_int64(ops, name, *v);
            }
            Field::I128(v) => {
                set_operation_str(ops, name, v.to_string(), allocated_strings);
            }
            Field::Float(v) => {
                as_operations_add_write_double(ops, name, v.0);
            }
            Field::Boolean(v) => {
                as_operations_add_write_bool(ops, name, *v);
            }
            Field::String(string) | Field::Text(string) => {
                set_operation_str(ops, name, string.to_owned(), allocated_strings);
            }
            Field::Binary(v) => {
                as_operations_add_write_rawp(ops, name, v.as_ptr(), v.len() as u32, false);
            }
            Field::Decimal(v) => {
                set_operation_str(ops, name, v.to_string(), allocated_strings);
            }
            Field::Timestamp(v) => {
                set_operation_str(ops, name, v.to_rfc3339(), allocated_strings);
            }
            // Date's display implementation is RFC3339 compatible
            Field::Date(v) => {
                set_operation_str(ops, name, v.to_string(), allocated_strings);
            }
            Field::Duration(DozerDuration(duration, _)) => {
                set_operation_str(
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

#[inline(always)]
fn map<T>(val: *mut as_val, typ: as_val_type_e, f: impl FnOnce(&T) -> Option<Field>) -> Field {
    as_util_fromval(val, typ)
        .map(|val| unsafe { val.as_ref() })
        .and_then(f)
        .unwrap_or(Field::Null)
}

pub(crate) fn parse_record(
    record: &as_record,
    schema: &Schema,
    bin_names: &BinNames,
) -> Result<Vec<Field>, Error> {
    let record = record as *const as_record;
    let mut values = Vec::with_capacity(schema.fields.len());
    for (field, name) in schema.fields.iter().zip(bin_names.names()) {
        let val = unsafe { as_record_get(record, name.as_ptr()) as *mut as_val };
        let v = if val.is_null() {
            Field::Null
        } else {
            match field.typ {
                dozer_types::types::FieldType::UInt => {
                    map(val, as_val_type_e_AS_INTEGER, |v: &as_integer| {
                        Some(Field::UInt(v.value.to_u64()?))
                    })
                }
                dozer_types::types::FieldType::U128 => {
                    map(val, as_val_type_e_AS_STRING, |v: &as_string| {
                        Some(Field::U128(unsafe {
                            CStr::from_ptr(v.value).to_str().ok()?.parse().ok()?
                        }))
                    })
                }
                dozer_types::types::FieldType::Int => {
                    map(val, as_val_type_e_AS_INTEGER, |v: &as_integer| {
                        Some(Field::Int(v.value))
                    })
                }
                dozer_types::types::FieldType::I128 => {
                    map(val, as_val_type_e_AS_STRING, |v: &as_string| {
                        Some(Field::I128(unsafe {
                            CStr::from_ptr(v.value).to_str().ok()?.parse().ok()?
                        }))
                    })
                }
                dozer_types::types::FieldType::Float => {
                    map(val, as_val_type_e_AS_DOUBLE, |v: &as_double| {
                        Some(Field::Float(OrderedFloat(v.value)))
                    })
                }
                dozer_types::types::FieldType::Boolean => {
                    map(val, as_val_type_e_AS_BOOLEAN, |v: &as_boolean| {
                        Some(Field::Boolean(v.value))
                    })
                }
                dozer_types::types::FieldType::String => {
                    map(val, as_val_type_e_AS_STRING, |v: &as_string| {
                        Some(Field::String(
                            unsafe { CStr::from_ptr(v.value) }.to_str().ok()?.to_owned(),
                        ))
                    })
                }
                dozer_types::types::FieldType::Text => {
                    map(val, as_val_type_e_AS_STRING, |v: &as_string| {
                        Some(Field::Text(
                            unsafe { CStr::from_ptr(v.value) }.to_str().ok()?.to_owned(),
                        ))
                    })
                }
                dozer_types::types::FieldType::Binary => {
                    map(val, as_val_type_e_AS_BYTES, |v: &as_bytes| {
                        Some(Field::Binary(unsafe {
                            slice::from_raw_parts(v.value, v.size as usize).to_vec()
                        }))
                    })
                }
                dozer_types::types::FieldType::Decimal => {
                    map(val, as_val_type_e_AS_STRING, |v: &as_string| {
                        Some(Field::Decimal(unsafe {
                            CStr::from_ptr(v.value).to_str().ok()?.parse().ok()?
                        }))
                    })
                }
                dozer_types::types::FieldType::Timestamp => {
                    map(val, as_val_type_e_AS_STRING, |v: &as_string| {
                        Some(Field::Timestamp(unsafe {
                            DateTime::parse_from_rfc3339(CStr::from_ptr(v.value).to_str().ok()?)
                                .ok()?
                        }))
                    })
                }

                dozer_types::types::FieldType::Date => {
                    map(val, as_val_type_e_AS_STRING, |v: &as_string| {
                        Some(Field::Date(unsafe {
                            NaiveDate::from_str(CStr::from_ptr(v.value).to_str().ok()?).ok()?
                        }))
                    })
                }
                dozer_types::types::FieldType::Point => unimplemented!(),
                dozer_types::types::FieldType::Duration => unimplemented!(),
                dozer_types::types::FieldType::Json => unimplemented!(),
            }
        };
        if !field.nullable && v == Field::Null {
            return Err(Error::NotNullNotFound);
        }
        values.push(v);
    }
    Ok(values)
}

#[inline(always)]
fn as_util_fromval<T>(v: *mut as_val, typ: as_val_type_e) -> Option<NonNull<T>> {
    unsafe {
        let v = NonNull::new(v)?;
        if v.as_ref().type_ != typ as u8 {
            return None;
        }
        Some(v.cast())
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
pub(crate) unsafe fn as_vector_get(vector: *const as_vector, index: usize) -> *const c_void {
    debug_assert!(index < (*vector).size as usize);
    (*vector)
        .list
        .byte_add((*vector).item_size as usize * index)
}

#[inline(always)]
pub(crate) unsafe fn as_batch_write_reserve(
    records: *mut as_batch_records,
) -> *mut as_batch_write_record {
    let r = as_vector_reserve(&mut (*records).list as *mut as_vector) as *mut as_batch_write_record;
    (*r).type_ = AS_BATCH_WRITE as u8;
    (*r).has_write = true;
    r
}

#[inline(always)]
pub(crate) unsafe fn as_batch_remove_reserve(
    records: *mut as_batch_records,
) -> *mut as_batch_remove_record {
    let r =
        as_vector_reserve(&mut (*records).list as *mut as_vector) as *mut as_batch_remove_record;
    (*r).type_ = AS_BATCH_REMOVE as u8;
    (*r).has_write = true;
    r
}

#[inline(always)]
pub(crate) unsafe fn as_batch_read_reserve(
    records: *mut as_batch_records,
) -> *mut as_batch_read_record {
    let r = as_vector_reserve(&mut (*records).list as *mut as_vector) as *mut as_batch_read_record;
    (*r).type_ = AS_BATCH_READ as u8;
    r
}

#[inline(always)]
pub(crate) unsafe fn as_batch_records_create(capacity: u32) -> *mut as_batch_records {
    as_vector_create(std::mem::size_of::<as_batch_record>() as u32, capacity)
        as *mut as_batch_records
}

pub(crate) struct AsOperations(*mut as_operations);
impl AsOperations {
    pub(crate) fn new(capacity: u16) -> Self {
        unsafe { Self(check_alloc(as_operations_new(capacity))) }
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut as_operations {
        self.0
    }
}

impl Drop for AsOperations {
    fn drop(&mut self) {
        unsafe { as_operations_destroy(self.0) }
    }
}
