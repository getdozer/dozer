use std::path::Path;

use dozer_cache::{
    cache::{index::get_primary_key, RwCache, RwCacheManager},
    errors::CacheError,
};
use dozer_core::executor::ExecutorOperation;
use dozer_types::{
    grpc_types::types::Operation as GrpcOperation,
    indicatif::MultiProgress,
    log::error,
    models::api_endpoint::ConflictResolution,
    types::{FieldDefinition, FieldType, IndexDefinition, Operation, Schema},
};
use futures_util::{Future, StreamExt};
use tokio::sync::broadcast::Sender;

use crate::grpc::types_helper;

pub use self::log_reader::LogReader;

mod log_reader;

pub fn create_cache(
    cache_manager: &dyn RwCacheManager,
    schema: Schema,
    log_path: &Path,
    conflict_resolution: ConflictResolution,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
    mullti_pb: Option<MultiProgress>,
) -> Result<(String, impl Future<Output = Result<(), CacheError>>), CacheError> {
    // Automatically create secondary indexes
    let secondary_indexes = generate_secondary_indexes(&schema.fields);
    // Create the cache.
    let cache =
        cache_manager.create_cache(schema.clone(), secondary_indexes, conflict_resolution)?;
    let name = cache.name().to_string();

    // Create log reader.
    let log_reader = LogReader::new(log_path, &name, 0, mullti_pb)?;

    // Spawn a task to write to cache.
    let task = build_cache(cache, log_reader, schema, operations_sender);
    Ok((name, task))
}

async fn build_cache(
    mut cache: Box<dyn RwCache>,
    mut log_reader: LogReader,
    schema: Schema,
    operations_sender: Option<(String, Sender<GrpcOperation>)>,
) -> Result<(), CacheError> {
    while let Some(executor_operation) = log_reader.next().await {
        match executor_operation {
            ExecutorOperation::Op { op } => match op {
                Operation::Delete { mut old } => {
                    old.schema_id = schema.identifier;
                    let key = get_primary_key(&schema.primary_index, &old.values);
                    let version = cache.delete(&key)?;
                    old.version = Some(version);

                    if let Some((endpoint_name, operations_sender)) = operations_sender.as_ref() {
                        let operation =
                            types_helper::map_delete_operation(endpoint_name.clone(), old);
                        send_and_log_error(operations_sender, operation);
                    }
                }
                Operation::Insert { mut new } => {
                    new.schema_id = schema.identifier;
                    let id = cache.insert(&mut new)?;

                    if let Some((endpoint_name, operations_sender)) = operations_sender.as_ref() {
                        let operation =
                            types_helper::map_insert_operation(endpoint_name.clone(), new, id);
                        send_and_log_error(operations_sender, operation);
                    }
                }
                Operation::Update { mut old, mut new } => {
                    old.schema_id = schema.identifier;
                    new.schema_id = schema.identifier;
                    let key = get_primary_key(&schema.primary_index, &old.values);
                    let (version, id) = cache.update(&key, &mut new)?;

                    if let Some((endpoint_name, operations_sender)) = operations_sender.as_ref() {
                        let operation = if let Some(version) = version {
                            old.version = Some(version);
                            types_helper::map_update_operation(endpoint_name.clone(), old, new)
                        } else {
                            types_helper::map_insert_operation(endpoint_name.clone(), new, id)
                        };
                        send_and_log_error(operations_sender, operation);
                    }
                }
            },
            ExecutorOperation::Commit { .. } => {
                cache.commit()?;
            }
            ExecutorOperation::SnapshottingDone {} => {
                cache.commit()?;
            }
            ExecutorOperation::Terminate => {
                break;
            }
        }
    }
    Ok(())
}

fn generate_secondary_indexes(fields: &[FieldDefinition]) -> Vec<IndexDefinition> {
    fields
        .iter()
        .enumerate()
        .flat_map(|(idx, f)| match f.typ {
            // Create sorted inverted indexes for these fields
            FieldType::UInt
            | FieldType::U128
            | FieldType::Int
            | FieldType::I128
            | FieldType::Float
            | FieldType::Boolean
            | FieldType::Decimal
            | FieldType::Timestamp
            | FieldType::Date
            | FieldType::Point
            | FieldType::Duration => vec![IndexDefinition::SortedInverted(vec![idx])],

            // Create sorted inverted and full text indexes for string fields.
            FieldType::String => vec![
                IndexDefinition::SortedInverted(vec![idx]),
                IndexDefinition::FullText(idx),
            ],

            // Create full text indexes for text fields
            // FieldType::Text => vec![IndexDefinition::FullText(idx)],
            FieldType::Text => vec![],

            // Skip creating indexes
            FieldType::Binary | FieldType::Bson => vec![],
        })
        .collect()
}

fn send_and_log_error<T: Send + Sync + 'static>(sender: &Sender<T>, msg: T) {
    if let Err(e) = sender.send(msg) {
        error!("Failed to send broadcast message: {}", e);
    }
}
