use std::{collections::HashMap, path::Path, sync::Arc};

use dozer_core::{
    dag::node::{PortHandle, Processor, ProcessorFactory},
    storage::{common::RenewableRwTransaction, lmdb_storage::LmdbEnvironmentManager},
};
use dozer_types::{parking_lot::RwLock, types::Schema};

use crate::{
    builder::get_select, errors::PipelineError, product::factory::ProductProcessorFactory,
};

type Transaction = dozer_types::parking_lot::lock_api::RwLock<
    dozer_types::parking_lot::RawRwLock,
    Box<dyn RenewableRwTransaction>,
>;

pub(crate) fn _init_processor(
    sql: &str,
    schemas: HashMap<PortHandle, Schema>,
) -> Result<(Box<dyn Processor>, Arc<Transaction>), PipelineError> {
    let select = get_select(sql)?;

    let processor_factory = ProductProcessorFactory::new(select.from[0].clone());

    let mut processor = processor_factory.build(schemas, HashMap::new());

    let mut storage = LmdbEnvironmentManager::create(
        tempdir::TempDir::new("test").unwrap().path(),
        "product_test",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let tx = Arc::new(RwLock::new(storage.create_txn().unwrap()));

    Ok((processor, tx))
}
