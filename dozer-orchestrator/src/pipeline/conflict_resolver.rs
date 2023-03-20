use dozer_cache::cache::index::get_primary_key;
use dozer_cache::errors::CacheError;
use dozer_cache::errors::CacheError::PrimaryKeyExists;
use dozer_types::types::{Operation, Record, Schema};

use dozer_types::log::warn;
use dozer_types::models::api_endpoint::{
    OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
};

pub struct ConflictResolver {}

impl ConflictResolver {
    pub fn resolve_insert_error(
        new: Record,
        schema: &Schema,
        err: CacheError,
        resolution: OnInsertResolutionTypes,
    ) -> Result<Option<Operation>, CacheError> {
        let key = get_primary_key(&schema.primary_index, &new.values);
        match (resolution, err) {
            (OnInsertResolutionTypes::Nothing, PrimaryKeyExists) => {
                warn!("Record (Key: {:?}) already exist, ignoring insert", key);
                Ok(None)
            }
            (OnInsertResolutionTypes::Update, PrimaryKeyExists) => {
                warn!("Record (Key: {:?}) already exist, trying update", key);
                let new_op = Operation::Update {
                    old: new.clone(),
                    new,
                };
                Ok(Some(new_op))
            }
            (_, e) => Err(e),
        }
    }

    pub fn resolve_update_error(
        new: Record,
        schema: &Schema,
        err: CacheError,
        resolution: OnUpdateResolutionTypes,
    ) -> Result<Option<Operation>, CacheError> {
        let key = get_primary_key(&schema.primary_index, &new.values);
        match (resolution, err) {
            (OnUpdateResolutionTypes::Nothing, CacheError::PrimaryKeyNotFound) => {
                warn!("Record (Key: {:?}) not found, ignoring update", key);
                Ok(None)
            }
            (OnUpdateResolutionTypes::Upsert, CacheError::PrimaryKeyNotFound) => {
                warn!("Record (Key: {:?}) not found, trying update", key);
                let insert_op = Operation::Insert { new };
                Ok(Some(insert_op))
            }
            (_, e) => Err(e),
        }
    }

    pub fn resolve_delete_error(
        old: Record,
        schema: &Schema,
        err: CacheError,
        resolution: OnDeleteResolutionTypes,
    ) -> Result<(), CacheError> {
        let key = get_primary_key(&schema.primary_index, &old.values);
        match (resolution, err) {
            (OnDeleteResolutionTypes::Nothing, CacheError::PrimaryKeyNotFound) => {
                warn!("Record (Key: {:?}) not found, ignoring delete", key);
                Ok(())
            }
            (_, e) => Err(e),
        }
    }
}
