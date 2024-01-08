use crate::tests::utils::create_test_runtime;
use crate::{projection::factory::ProjectionProcessorFactory, tests::utils::get_select};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::node::ProcessorFactory;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_recordstore::ProcessorRecordStoreDeserializer;
use dozer_types::types::{Field, Schema};
use dozer_types::types::{Operation, Record};
use std::collections::HashMap;

struct TestChannelForwarder {
    operations: Vec<Operation>,
}

impl ProcessorChannelForwarder for TestChannelForwarder {
    fn send(&mut self, op: Operation, _port: dozer_core::node::PortHandle) {
        self.operations.push(op);
    }
}

pub(crate) fn run_fct(sql: &str, schema: Schema, input: Vec<Field>) -> Field {
    let record_store = ProcessorRecordStoreDeserializer::new(Default::default()).unwrap();

    let select = get_select(sql).unwrap();
    let runtime = create_test_runtime();
    let processor_factory = ProjectionProcessorFactory::_new(
        "projection_id".to_owned(),
        select.projection,
        vec![],
        runtime.clone(),
    );
    runtime
        .block_on(
            processor_factory.get_output_schema(
                &DEFAULT_PORT_HANDLE,
                &[(DEFAULT_PORT_HANDLE, schema.clone())]
                    .into_iter()
                    .collect(),
            ),
        )
        .unwrap();

    let mut processor = runtime
        .block_on(processor_factory.build(
            HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
            HashMap::new(),
            &record_store,
            None,
        ))
        .unwrap();

    let record_store = record_store.into_record_store();

    let mut fw = TestChannelForwarder { operations: vec![] };
    let rec = Record::new(input);

    let op = Operation::Insert { new: rec };

    processor
        .process(DEFAULT_PORT_HANDLE, &record_store, op, &mut fw)
        .unwrap();

    match &mut fw.operations[0] {
        Operation::Insert { new } => new.values.remove(0),
        _ => panic!("Unable to find result value"),
    }
}
