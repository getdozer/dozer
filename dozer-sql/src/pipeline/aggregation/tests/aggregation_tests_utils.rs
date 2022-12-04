use std::{path::Path, sync::Arc};

use dozer_core::{
    dag::node::Processor,
    storage::{common::RenewableRwTransaction, lmdb_storage::LmdbEnvironmentManager},
};
use dozer_types::parking_lot::RwLock;

use crate::pipeline::{
    aggregation::processor::AggregationProcessor, builder::get_select, errors::PipelineError,
};

type AggregationTransaction = dozer_types::parking_lot::lock_api::RwLock<
    dozer_types::parking_lot::RawRwLock,
    Box<dyn RenewableRwTransaction>,
>;

pub(crate) fn init_processor(
    sql: &str,
) -> Result<(AggregationProcessor, Arc<AggregationTransaction>), PipelineError> {
    let select = get_select(sql)?;

    let mut processor =
        AggregationProcessor::new(select.projection.clone(), select.group_by.clone());

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let tx = Arc::new(RwLock::new(storage.create_txn().unwrap()));

    Ok((processor, tx))
}
