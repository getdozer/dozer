use std::{collections::HashMap, path::Path, sync::Arc};

use dozer_core::{
    dag::{
        dag::DEFAULT_PORT_HANDLE,
        node::{PortHandle, Processor},
    },
    storage::{common::RenewableRwTransaction, lmdb_storage::LmdbEnvironmentManager},
};
use dozer_types::{parking_lot::RwLock, types::Schema};

use crate::pipeline::{
    aggregation::{factory::get_aggregation_rules, processor::AggregationProcessor},
    builder::get_select,
    errors::PipelineError,
};

type AggregationTransaction = dozer_types::parking_lot::lock_api::RwLock<
    dozer_types::parking_lot::RawRwLock,
    Box<dyn RenewableRwTransaction>,
>;

pub(crate) fn init_processor(
    sql: &str,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<(AggregationProcessor, Arc<AggregationTransaction>), PipelineError> {
    let select = get_select(sql)?;

    let input_schema = input_schemas
        .get(&DEFAULT_PORT_HANDLE)
        .unwrap_or_else(|| panic!("Error getting Input Schema"));

    let output_field_rules = get_aggregation_rules(
        &select.projection.clone(),
        &select.group_by.clone(),
        input_schema,
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let mut processor = AggregationProcessor::new(output_field_rules, input_schema);

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let tx = Arc::new(RwLock::new(storage.create_txn().unwrap()));

    Ok((processor, tx))
}
