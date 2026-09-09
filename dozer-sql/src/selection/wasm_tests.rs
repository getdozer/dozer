use std::collections::HashMap;

use dozer_core::{
    channels::ProcessorChannelForwarder, event::EventHub, node::ProcessorFactory,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    models::udf_config::{UdfConfig, UdfType, WasmConfig},
    types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    },
};

use crate::tests::utils::{create_test_runtime, get_select};

use super::factory::SelectionProcessorFactory;

fn fixture() -> (tempfile::NamedTempFile, UdfConfig) {
    let module = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        module.path(),
        wat::parse_str(
            r#"(module
        (func (export "increment") (param i32) (result i32)
            local.get 0 i32.const 1 i32.add))"#,
        )
        .unwrap(),
    )
    .unwrap();
    let config = UdfConfig {
        name: "increment".into(),
        config: UdfType::Wasm(WasmConfig {
            path: module.path().to_str().unwrap().into(),
            function: None,
        }),
    };
    (module, config)
}

fn schema(typ: FieldType) -> Schema {
    Schema::default()
        .field(
            FieldDefinition::new("value".into(), typ, true, SourceDefinition::Dynamic),
            false,
        )
        .to_owned()
}

#[test]
fn where_factory_rejects_invalid_wasm_arguments_before_processing_rows() {
    let (_module, config) = fixture();
    let runtime = create_test_runtime();
    let select = get_select("SELECT value FROM input WHERE increment(value) > 0").unwrap();
    let factory = SelectionProcessorFactory::new(
        "selection".into(),
        select.selection.unwrap(),
        vec![config],
        runtime.clone(),
    );
    let result = runtime.block_on(factory.build(
        HashMap::from([(DEFAULT_PORT_HANDLE, schema(FieldType::String))]),
        HashMap::new(),
        EventHub::new(1),
    ));
    let Err(error) = result else {
        panic!("a string argument must fail during factory build")
    };
    assert!(error.to_string().contains("increment"));
}

#[derive(Default)]
struct Forwarder(Vec<TableOperation>);

impl ProcessorChannelForwarder for Forwarder {
    fn send(&mut self, operation: TableOperation) {
        self.0.push(operation);
    }
}

#[test]
fn where_wasm_filters_int8_and_null_rows_and_handles_updates() {
    let (_module, config) = fixture();
    let runtime = create_test_runtime();
    let select = get_select("SELECT value FROM input WHERE increment(value) > 0").unwrap();
    let factory = SelectionProcessorFactory::new(
        "selection".into(),
        select.selection.unwrap(),
        vec![config],
        runtime.clone(),
    );
    let mut processor = runtime
        .block_on(factory.build(
            HashMap::from([(DEFAULT_PORT_HANDLE, schema(FieldType::Int8))]),
            HashMap::new(),
            EventHub::new(1),
        ))
        .unwrap();
    let record = |field| Record::new(vec![field]);
    let mut forwarder = Forwarder::default();
    for op in [
        Operation::Insert {
            new: record(Field::Int8(-1)),
        },
        Operation::Insert {
            new: record(Field::Null),
        },
        Operation::Insert {
            new: record(Field::Int8(0)),
        },
        Operation::Update {
            old: record(Field::Int8(-1)),
            new: record(Field::Int8(2)),
        },
        Operation::Update {
            old: record(Field::Int8(2)),
            new: record(Field::Int8(-1)),
        },
    ] {
        processor
            .process(
                TableOperation::without_id(op, DEFAULT_PORT_HANDLE),
                &mut forwarder,
            )
            .unwrap();
    }
    let operations: Vec<_> = forwarder.0.into_iter().map(|op| op.op).collect();
    assert_eq!(
        operations,
        vec![
            Operation::Insert {
                new: record(Field::Int8(0))
            },
            Operation::Insert {
                new: record(Field::Int8(2))
            },
            Operation::Delete {
                old: record(Field::Int8(2))
            },
        ]
    );
}
