use crate::{projection::factory::ProjectionProcessorFactory, tests::utils::get_select};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::ProcessorFactory;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_recordstore::ProcessorRecordStore;
use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};
use std::collections::HashMap;

struct TestChannelForwarder {
    operations: Vec<ProcessorOperation>,
}

impl ProcessorChannelForwarder for TestChannelForwarder {
    fn send(&mut self, op: ProcessorOperation, _port: dozer_core::node::PortHandle) {
        self.operations.push(op);
    }
}

pub(crate) fn run_fct(sql: &str, schema: Schema, input: Vec<Field>) -> Field {
    let record_store = ProcessorRecordStore::new().unwrap();

    let select = get_select(sql).unwrap();
    let processor_factory =
        ProjectionProcessorFactory::_new("projection_id".to_owned(), select.projection, vec![]);
    processor_factory
        .get_output_schema(
            &DEFAULT_PORT_HANDLE,
            &[(DEFAULT_PORT_HANDLE, schema.clone())]
                .into_iter()
                .collect(),
        )
        .unwrap();

    let mut processor = processor_factory
        .build(
            HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
            HashMap::new(),
            &record_store,
            None,
        )
        .unwrap();

    let mut fw = TestChannelForwarder { operations: vec![] };
    let rec = Record::new(input);
    let rec = record_store.create_record(&rec).unwrap();

    let op = ProcessorOperation::Insert { new: rec };

    processor
        .process(DEFAULT_PORT_HANDLE, &record_store, op, &mut fw)
        .unwrap();

    match &fw.operations[0] {
        ProcessorOperation::Insert { new } => {
            let mut new = record_store.load_record(new).unwrap();
            new.values.remove(0)
        }
        _ => panic!("Unable to find result value"),
    }
}
