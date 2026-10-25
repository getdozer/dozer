use crate::tests::utils::{create_test_runtime, get_select};
use dozer_sql_expression::builder::ExpressionBuilder;
use dozer_types::{models::config::Config, types::Schema};

fn config() -> Config {
    config_with_format(false)
}

fn config_with_format(explicit_type: bool) -> Config {
    let yaml = include_str!("../../../../examples/wasm-udf/dozer-config.yaml");
    let yaml = if explicit_type {
        assert_eq!(
            yaml.matches("  wasm:\n    my_function: udfs/wasm/my_function")
                .count(),
            1
        );
        yaml.replace(
            "  wasm:\n    my_function: udfs/wasm/my_function",
            "  - type: wasm\n    name: my_function\n    config:\n      path: udfs/wasm/my_function",
        )
    } else {
        yaml.to_owned()
    };
    let mut config: Config = dozer_types::serde_yaml::from_str(&yaml).unwrap();
    for udf in &mut config.udfs {
        if let dozer_types::models::udf_config::UdfType::Wasm(wasm) = &mut udf.config {
            wasm.path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../examples/wasm-udf")
                .join(&wasm.path)
                .to_str()
                .unwrap()
                .into();
        }
    }
    config
}

#[test]
#[cfg(not(feature = "wasm"))]
fn disabled_wasm_reports_feature_requirement() {
    use dozer_sql_expression::{error::Error, sqlparser::ast::SelectItem};
    let select = get_select("SELECT my_function(3,4) FROM numbers").unwrap();
    let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
        panic!("expected expression")
    };
    let runtime = create_test_runtime();
    for config in [config(), config_with_format(true)] {
        let error = runtime
            .block_on(ExpressionBuilder::new(0, runtime.clone()).build(
                false,
                expression,
                &Schema::default(),
                &config.udfs,
            ))
            .unwrap_err();
        assert!(matches!(error, Error::WasmNotEnabled));
    }
}

#[cfg(feature = "wasm")]
mod enabled {
    use super::*;
    use crate::{
        planner::projection::CommonPlanner, projection::processor::ProjectionProcessor,
        selection::factory::SelectionProcessorFactory,
    };
    use dozer_core::{
        channels::ProcessorChannelForwarder,
        event::EventHub,
        node::{Processor, ProcessorFactory},
        DEFAULT_PORT_HANDLE,
    };
    use dozer_sql_expression::udf_registry::UdfRegistry;
    use dozer_sql_expression::{execution::Expression, sqlparser::ast::SelectItem};
    use dozer_types::types::{
        Field, FieldDefinition, FieldType, Operation, Record, SourceDefinition, TableOperation,
    };
    use std::collections::HashMap;

    #[test]
    fn prepared_snapshot_is_used_by_schema_planning_and_processors() {
        use crate::aggregation::factory::AggregationProcessorFactory;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("my_function.wasm");
        std::fs::write(
            &path,
            include_bytes!("../../../../examples/wasm-udf/udfs/wasm/my_function.wasm"),
        )
        .unwrap();
        let mut config = config();
        if let dozer_types::models::udf_config::UdfType::Wasm(wasm) = &mut config.udfs[0].config {
            wasm.path = path.to_str().unwrap().into();
        }
        let registry = UdfRegistry::from(config.udfs);
        registry.prepare().unwrap();
        std::fs::remove_file(&path).unwrap();
        let runtime = create_test_runtime();
        let select =
            get_select("SELECT my_function(a,b) FROM numbers WHERE my_function(a,b) > 6").unwrap();
        let filter = SelectionProcessorFactory::new(
            "filter".into(),
            select.selection.unwrap(),
            registry.clone(),
            runtime.clone(),
        );
        let projection = AggregationProcessorFactory::new(
            "project".into(),
            select.projection,
            select.group_by,
            select.having,
            false,
            registry.clone(),
            runtime.clone(),
        );
        let inputs = HashMap::from([(DEFAULT_PORT_HANDLE, schema())]);
        for factory in [
            &filter as &dyn ProcessorFactory,
            &projection as &dyn ProcessorFactory,
        ] {
            let output_schema = runtime
                .block_on(factory.get_output_schema(&DEFAULT_PORT_HANDLE, &inputs))
                .unwrap();
            let mut processor = runtime
                .block_on(factory.build(
                    inputs.clone(),
                    HashMap::from([(DEFAULT_PORT_HANDLE, output_schema)]),
                    EventHub::new(1),
                ))
                .unwrap();
            let mut output = Collector::default();
            processor
                .process(
                    TableOperation::without_id(
                        Operation::Insert { new: row(3, 4) },
                        DEFAULT_PORT_HANDLE,
                    ),
                    &mut output,
                )
                .unwrap();
            let expected = if factory.id() == "filter" {
                row(3, 4)
            } else {
                Record::new(vec![Field::Int(7)])
            };
            assert_eq!(output.0, vec![Operation::Insert { new: expected }]);
        }
        for sql in [
            "SELECT my_function(my_function(a,b),1) FROM numbers",
            "SELECT my_function(a,b),COUNT(*) FROM numbers GROUP BY my_function(a,b) HAVING my_function(a,b) > 1",
        ] {
            let select = get_select(sql).unwrap();
            let mut planner = CommonPlanner::with_udf_registry(schema(), &registry, runtime.clone());
            runtime.block_on(planner.plan(select.projection, select.group_by, select.having)).unwrap();
        }
    }

    #[test]
    fn where_arguments_are_validated_during_schema_propagation() {
        let runtime = create_test_runtime();
        for sql in [
            "SELECT a FROM numbers WHERE my_function(a) > 0",
            "SELECT a FROM numbers WHERE my_function('bad',b) > 0",
        ] {
            let select = get_select(sql).unwrap();
            let factory = SelectionProcessorFactory::new(
                "filter".into(),
                select.selection.unwrap(),
                config().udfs,
                runtime.clone(),
            );
            assert!(runtime
                .block_on(factory.get_output_schema(
                    &DEFAULT_PORT_HANDLE,
                    &HashMap::from([(DEFAULT_PORT_HANDLE, schema())])
                ))
                .is_err());
        }
    }

    #[test]
    fn unused_invalid_module_is_rejected_by_pipeline_planning() {
        use crate::builder::statement_to_pipeline;
        use dozer_core::app::AppPipeline;
        let mut config = config();
        if let dozer_types::models::udf_config::UdfType::Wasm(wasm) = &mut config.udfs[0].config {
            wasm.path.push_str("-missing");
        }
        let result = statement_to_pipeline(
            "SELECT a INTO output FROM numbers",
            &mut AppPipeline::new_with_default_flags(),
            None,
            config.udfs,
            create_test_runtime(),
        );
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("cannot initialize WASM UDF my_function"));
    }

    #[test]
    fn actual_assemblyscript_integer_float_and_boolean_exports() {
        use dozer_types::{
            models::udf_config::{UdfConfig, UdfType, WasmConfig},
            ordered_float::OrderedFloat,
        };
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/wasm-udf/udfs/wasm/primitives.wasm");
        let registry = UdfRegistry::from(
            ["keep_integer", "half", "single", "invert"]
                .into_iter()
                .map(|name| UdfConfig {
                    name: name.into(),
                    config: UdfType::Wasm(WasmConfig {
                        path: path.to_str().unwrap().into(),
                    }),
                })
                .collect::<Vec<_>>(),
        );
        registry.prepare().unwrap();
        let runtime = create_test_runtime();
        for (name, ty, input, expected) in [
            (
                "keep_integer",
                FieldType::Int,
                Field::Int(i64::MIN),
                Field::Int(i64::MIN),
            ),
            (
                "half",
                FieldType::Float,
                Field::Float(OrderedFloat(3.0)),
                Field::Float(OrderedFloat(1.5)),
            ),
            (
                "single",
                FieldType::Float,
                Field::Float(OrderedFloat(0.1)),
                Field::Float(OrderedFloat(f64::from(0.1f32))),
            ),
            (
                "invert",
                FieldType::Boolean,
                Field::Boolean(true),
                Field::Int(0),
            ),
            (
                "invert",
                FieldType::Boolean,
                Field::Boolean(false),
                Field::Int(1),
            ),
        ] {
            let schema = Schema::default()
                .field(
                    FieldDefinition::new("value".into(), ty, false, SourceDefinition::Dynamic),
                    false,
                )
                .to_owned();
            let select = get_select(&format!("SELECT {name}(value) FROM numbers")).unwrap();
            let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
                panic!("expected expression")
            };
            let mut compiled = runtime
                .block_on(
                    ExpressionBuilder::new(0, runtime.clone())
                        .with_udf_registry(registry.clone())
                        .build(false, expression, &schema, &registry),
                )
                .unwrap();
            assert_eq!(
                compiled
                    .evaluate(&Record::new(vec![input.clone()]), &schema)
                    .unwrap(),
                expected
            );
            if name == "invert" {
                let select =
                    get_select("SELECT CAST(invert(value) AS BOOLEAN) FROM numbers").unwrap();
                let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
                    panic!("expected expression")
                };
                let mut cast = runtime
                    .block_on(
                        ExpressionBuilder::new(0, runtime.clone())
                            .with_udf_registry(registry.clone())
                            .build(false, expression, &schema, &registry),
                    )
                    .unwrap();
                assert_eq!(
                    cast.get_type(&schema).unwrap().return_type,
                    FieldType::Boolean
                );
                assert_eq!(
                    cast.evaluate(&Record::new(vec![input]), &schema).unwrap(),
                    Field::Boolean(expected == Field::Int(1))
                );
            }
        }
    }

    #[derive(Default)]
    struct Collector(Vec<Operation>);
    impl ProcessorChannelForwarder for Collector {
        fn send(&mut self, operation: TableOperation) {
            self.0.push(operation.op);
        }
    }

    fn schema() -> Schema {
        Schema::default()
            .field(
                FieldDefinition::new("a".into(), FieldType::Int, true, SourceDefinition::Dynamic),
                false,
            )
            .field(
                FieldDefinition::new("b".into(), FieldType::Int, true, SourceDefinition::Dynamic),
                false,
            )
            .to_owned()
    }
    fn row(a: i64, b: i64) -> Record {
        Record::new(vec![Field::Int(a), Field::Int(b)])
    }
    fn result(sum: i64) -> Record {
        Record::new(vec![Field::Int(sum)])
    }

    #[test]
    fn assemblyscript_streaming_projection() {
        check_streaming_projection(config());
    }

    #[test]
    fn explicit_type_assemblyscript_streaming_projection() {
        check_streaming_projection(config_with_format(true));
    }

    fn check_streaming_projection(config: Config) {
        let runtime = create_test_runtime();
        let select = get_select(config.sql.as_ref().unwrap()).unwrap();
        let mut planner = CommonPlanner::new(schema(), &config.udfs, runtime.clone());
        runtime
            .block_on(planner.plan(select.projection, select.group_by, select.having))
            .unwrap();
        assert_eq!(planner.post_projection_schema.fields[0].typ, FieldType::Int);
        assert!(planner.post_projection_schema.fields[0].nullable);
        let mut processor = ProjectionProcessor::new(schema(), planner.projection_output).unwrap();
        let mut output = Collector::default();
        for operation in [
            Operation::Insert { new: row(3, 4) },
            Operation::Update {
                old: row(3, 4),
                new: row(3, 5),
            },
            Operation::Delete { old: row(3, 5) },
            Operation::BatchInsert {
                new: vec![row(-2, 3), Record::new(vec![Field::Null, Field::Int(4)])],
            },
        ] {
            processor
                .process(
                    TableOperation::without_id(operation, DEFAULT_PORT_HANDLE),
                    &mut output,
                )
                .unwrap();
        }
        assert_eq!(
            output.0,
            vec![
                Operation::Insert { new: result(7) },
                Operation::Update {
                    old: result(7),
                    new: result(8)
                },
                Operation::Delete { old: result(8) },
                Operation::BatchInsert {
                    new: vec![result(1), Record::new(vec![Field::Null])]
                },
            ]
        );
        println!("AssemblyScript SQL projection: {:?}", output.0);
    }

    #[test]
    fn assemblyscript_filter_enter_leave_update_delete() {
        let config = config();
        let runtime = create_test_runtime();
        let select = get_select("SELECT a,b FROM numbers WHERE my_function(a,b) > 7").unwrap();
        let factory = SelectionProcessorFactory::new(
            "wasm_filter".into(),
            select.selection.unwrap(),
            config.udfs,
            runtime.clone(),
        );
        let mut processor = runtime
            .block_on(factory.build(
                HashMap::from([(DEFAULT_PORT_HANDLE, schema())]),
                HashMap::new(),
                EventHub::new(1),
            ))
            .unwrap();
        let mut output = Collector::default();
        for operation in [
            Operation::Insert { new: row(3, 4) },
            Operation::Update {
                old: row(3, 4),
                new: row(3, 5),
            },
            Operation::Update {
                old: row(3, 5),
                new: row(3, 6),
            },
            Operation::Update {
                old: row(3, 6),
                new: row(3, 4),
            },
            Operation::Delete { old: row(3, 6) },
            Operation::Delete { old: row(3, 4) },
        ] {
            processor
                .process(
                    TableOperation::without_id(operation, DEFAULT_PORT_HANDLE),
                    &mut output,
                )
                .unwrap();
        }
        assert_eq!(
            output.0,
            vec![
                Operation::Insert { new: row(3, 5) },
                Operation::Update {
                    old: row(3, 5),
                    new: row(3, 6)
                },
                Operation::Delete { old: row(3, 6) },
                Operation::Delete { old: row(3, 6) },
            ]
        );
    }

    #[test]
    fn sql_nested_calls_case_and_literal_null() {
        let config = config();
        let runtime = create_test_runtime();
        for (sql, expected) in [
            (
                "SELECT MY_FUNCTION(my_function(a,b),1) FROM numbers",
                Field::Int(8),
            ),
            ("SELECT my_function(NULL,b) FROM numbers", Field::Null),
            (
                "SELECT CASE WHEN a > 0 THEN my_function(a,b) ELSE 0 END FROM numbers",
                Field::Int(7),
            ),
        ] {
            let select = get_select(sql).unwrap();
            let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
                panic!("expected expression")
            };
            let mut compiled = runtime
                .block_on(ExpressionBuilder::new(0, runtime.clone()).build(
                    false,
                    expression,
                    &schema(),
                    &config.udfs,
                ))
                .unwrap();
            assert_eq!(compiled.evaluate(&row(3, 4), &schema()).unwrap(), expected);
        }
    }

    #[test]
    fn planner_rejects_bad_arguments_and_missing_module() {
        let mut config = config();
        let runtime = create_test_runtime();
        for sql in [
            "SELECT my_function(a) FROM numbers",
            "SELECT my_function('text',b) FROM numbers",
            "SELECT my_function(*) FROM numbers",
            "SELECT my_function(DISTINCT a,b) FROM numbers",
            "SELECT my_function(a,b) OVER () FROM numbers",
            "SELECT my_function(a,b ORDER BY a) FROM numbers",
        ] {
            let select = get_select(sql).unwrap();
            let mut planner = CommonPlanner::new(schema(), &config.udfs, runtime.clone());
            assert!(
                runtime
                    .block_on(planner.plan(select.projection, select.group_by, select.having))
                    .is_err(),
                "accepted {sql}"
            );
        }
        if let dozer_types::models::udf_config::UdfType::Wasm(wasm) = &mut config.udfs[0].config {
            wasm.path.push_str("-missing");
        }
        let select = get_select(config.sql.as_ref().unwrap()).unwrap();
        let mut planner = CommonPlanner::new(schema(), &config.udfs, runtime.clone());
        assert!(runtime
            .block_on(planner.plan(select.projection, select.group_by, select.having))
            .is_err());
    }

    #[test]
    fn wasm_can_feed_an_aggregate() {
        let config = config();
        let runtime = create_test_runtime();
        let select = get_select("SELECT SUM(my_function(a,b)) FROM numbers").unwrap();
        let mut planner = CommonPlanner::new(schema(), &config.udfs, runtime.clone());
        runtime
            .block_on(planner.plan(select.projection, select.group_by, select.having))
            .unwrap();
        assert_eq!(planner.aggregation_output.len(), 1);
        let Expression::AggregateFunction { args, .. } = &mut planner.aggregation_output[0] else {
            panic!("expected aggregate")
        };
        assert_eq!(
            args[0].evaluate(&row(3, 4), &schema()).unwrap(),
            Field::Int(7)
        );
    }
}
