use dozer_cache::cache::index::get_primary_key;
use dozer_cache::errors::CacheError;
use dozer_cache::errors::CacheError::PrimaryKeyExists;
use dozer_types::types::{Operation, Record, Schema};

use dozer_types::log::warn;
use dozer_types::models::api_endpoint::{
    ConflictResolution, OnDeleteResolutionTypes, OnInsertResolutionTypes, OnUpdateResolutionTypes,
};

pub struct ConflictResolver {}

impl ConflictResolver {
    pub fn resolve_insert_error(
        new: Record,
        schema: &Schema,
        err: CacheError,
        resolution: Option<ConflictResolution>,
    ) -> Result<Option<Operation>, CacheError> {
        let insert_resolution =
            resolution.map_or(OnInsertResolutionTypes::Nothing.to_string(), |resolution| {
                resolution
                    .on_insert
                    .map_or(OnInsertResolutionTypes::Nothing.to_string(), |r| r)
            });

        let key = get_primary_key(&schema.primary_index, &new.values);
        match (insert_resolution, err) {
            (resolution_type, PrimaryKeyExists)
                if resolution_type == OnInsertResolutionTypes::Nothing.to_string() =>
            {
                warn!("Record (Key: {:?}) already exist, ignoring insert", key);
                Ok(None)
            }
            (resolution_type, PrimaryKeyExists)
                if resolution_type == OnInsertResolutionTypes::Update.to_string() =>
            {
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
        resolution: Option<ConflictResolution>,
    ) -> Result<Option<Operation>, CacheError> {
        let update_resolution =
            resolution.map_or(OnUpdateResolutionTypes::Nothing.to_string(), |resolution| {
                resolution
                    .on_update
                    .map_or(OnUpdateResolutionTypes::Nothing.to_string(), |r| r)
            });

        let key = get_primary_key(&schema.primary_index, &new.values);
        match (update_resolution, err) {
            (resolution_type, CacheError::PrimaryKeyNotFound)
                if resolution_type == OnUpdateResolutionTypes::Nothing.to_string() =>
            {
                warn!("Record (Key: {:?}) not found, ignoring update", key);
                Ok(None)
            }
            (resolution_type, CacheError::PrimaryKeyNotFound)
                if resolution_type == OnUpdateResolutionTypes::Upsert.to_string() =>
            {
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
        resolution: Option<ConflictResolution>,
    ) -> Result<(), CacheError> {
        let delete_resolution =
            resolution.map_or(OnDeleteResolutionTypes::Nothing.to_string(), |resolution| {
                resolution
                    .on_delete
                    .map_or(OnDeleteResolutionTypes::Nothing.to_string(), |r| r)
            });

        let key = get_primary_key(&schema.primary_index, &old.values);
        match (delete_resolution, err) {
            (resolution_type, CacheError::PrimaryKeyNotFound)
                if resolution_type == OnDeleteResolutionTypes::Nothing.to_string() =>
            {
                warn!("Record (Key: {:?}) not found, ignoring delete", key);
                Ok(())
            }
            (_, e) => Err(e),
        }
    }
}
