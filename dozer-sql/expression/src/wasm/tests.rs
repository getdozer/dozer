use super::*;
use crate::error::Error as ExpressionError;

fn load(bytes: &[u8], args: Vec<Field>) -> Result<Udf, ExpressionError> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.wasm");
    std::fs::write(&path, bytes).unwrap();
    Udf::new(
        "f".into(),
        &WasmConfig {
            path: path.to_str().unwrap().into(),
        },
        args.into_iter().map(Expression::Literal).collect(),
        &Schema::default(),
    )
}

fn udf(wat: &str, args: Vec<Field>) -> Result<Udf, ExpressionError> {
    load(&wat::parse_str(wat).unwrap(), args)
}

fn evaluate(udf: &mut Udf) -> Result<Field, ExpressionError> {
    udf.evaluate(&Record::new(vec![]), &Schema::default())
}

#[test]
fn numeric_abi_and_inferred_types() {
    for (wasm_type, input, output_type) in [
        ("i32", Field::Int(-17), FieldType::Int),
        ("i64", Field::Int(i64::MIN), FieldType::Int),
        ("f32", Field::Float(OrderedFloat(1.5)), FieldType::Float),
        ("f64", Field::Float(OrderedFloat(-2.5)), FieldType::Float),
    ] {
        let mut f = udf(&format!("(module (func (export \"f\") (param {wasm_type}) (result {wasm_type}) local.get 0))"), vec![input.clone()]).unwrap();
        assert_eq!(f.get_type().return_type, output_type);
        assert!(!f.get_type().nullable);
        assert_eq!(evaluate(&mut f).unwrap(), input);
        assert_eq!(f, f.clone());
    }
}

#[test]
fn boolean_inputs_use_the_assemblyscript_i32_abi() {
    for (input, expected) in [(false, 0), (true, 1)] {
        let mut f = udf(
            "(module (func (export \"f\") (param i32) (result i32) local.get 0))",
            vec![Field::Boolean(input)],
        )
        .unwrap();
        assert_eq!(f.get_type().return_type, FieldType::Int);
        assert_eq!(evaluate(&mut f).unwrap(), Field::Int(expected));
        for target in ["i64", "f32", "f64"] {
            assert!(udf(
                &format!(
                    "(module (func (export \"f\") (param {target}) (result {target}) local.get 0))"
                ),
                vec![Field::Boolean(input)]
            )
            .is_err());
        }
    }
}

#[test]
fn narrowing_conversions_warn_at_binding_and_not_per_record() {
    use dozer_types::log::{self, Level, LevelFilter, Log, Metadata};
    use std::{sync::Mutex, thread::ThreadId};
    type Capture = Option<(ThreadId, Vec<String>)>;
    struct Logger(Mutex<Capture>);
    impl Log for Logger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() == Level::Warn
        }
        fn log(&self, record: &log::Record) {
            if !self.enabled(record.metadata()) {
                return;
            }
            let mut capture = self.0.lock().unwrap();
            if let Some((thread, messages)) = capture.as_mut() {
                if *thread == std::thread::current().id() {
                    messages.push(record.args().to_string());
                }
            }
        }
        fn flush(&self) {}
    }
    static LOGGER: Logger = Logger(Mutex::new(None));
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(LevelFilter::Warn);
    *LOGGER.0.lock().unwrap() = Some((std::thread::current().id(), vec![]));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("f.wasm");
    for (target, input, warning) in [
        ("i32", Field::Int(1), true),
        ("i32", Field::UInt(1), true),
        ("i32", Field::I128(1), true),
        ("i32", Field::U128(1), true),
        ("i64", Field::UInt(1), true),
        ("i64", Field::I128(1), true),
        ("i64", Field::U128(1), true),
        ("f32", Field::Float(OrderedFloat(0.1)), true),
        ("i32", Field::Int8(1), false),
        ("i32", Field::Boolean(true), false),
        ("i64", Field::Int(1), false),
        ("f64", Field::Float(OrderedFloat(0.1)), false),
    ] {
        LOGGER.0.lock().unwrap().as_mut().unwrap().1.clear();
        let bytes = wat::parse_str(format!(
            "(module (func (export \"f\") (param {target}) (result {target}) local.get 0))"
        ))
        .unwrap();
        std::fs::write(&path, bytes).unwrap();
        // Literal Int8 is widened to Int by existing SQL literal typing. Use
        // a column schema to test the actual Int8 conversion without that rule.
        let input_type = if matches!(input, Field::Int8(_)) {
            FieldType::Int8
        } else {
            input.ty().unwrap()
        };
        let schema = Schema::default()
            .field(
                dozer_types::types::FieldDefinition::new(
                    "value".into(),
                    input_type,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .to_owned();
        let mut f = Udf::new(
            "f".into(),
            &WasmConfig {
                path: path.to_str().unwrap().into(),
            },
            vec![Expression::Column { index: 0 }],
            &schema,
        )
        .unwrap();
        let after_binding = LOGGER.0.lock().unwrap().as_ref().unwrap().1.clone();
        assert_eq!(after_binding.len(), usize::from(warning));
        if warning {
            assert!(after_binding[0].contains("WASM f argument 0:"));
        }
        for _ in 0..10 {
            f.evaluate(&Record::new(vec![input.clone()]), &schema)
                .unwrap();
        }
        assert_eq!(LOGGER.0.lock().unwrap().as_ref().unwrap().1, after_binding);
    }
    *LOGGER.0.lock().unwrap() = None;
}

#[test]
fn checked_integer_boundaries() {
    for (ty, min, max) in [
        ("i32", i32::MIN as i128, i32::MAX as i128),
        ("i64", i64::MIN as i128, i64::MAX as i128),
    ] {
        let module =
            format!("(module (func (export \"f\") (param {ty}) (result {ty}) local.get 0))");
        for value in [
            Field::I128(min),
            Field::I128(max),
            Field::UInt(max as u64),
            Field::U128(max as u128),
            Field::Int8(-1),
        ] {
            let mut f = udf(&module, vec![value.clone()]).unwrap();
            let expected = match value {
                Field::Int8(_) => -1,
                Field::I128(n) => n as i64,
                _ => max as i64,
            };
            assert_eq!(evaluate(&mut f).unwrap(), Field::Int(expected));
        }
        for value in [
            Field::I128(min - 1),
            Field::I128(max + 1),
            Field::UInt(u64::MAX),
            Field::U128(u128::MAX),
        ] {
            let mut f = udf(&module, vec![value]).unwrap();
            assert!(matches!(
                evaluate(&mut f),
                Err(ExpressionError::Wasm(Error::Argument { .. }))
            ));
        }
    }
}

#[test]
fn float_range_rounding_and_nonfinite_values() {
    let module = "(module (func (export \"f\") (param f32) (result f32) local.get 0))";
    let mut f = udf(module, vec![Field::Float(OrderedFloat(0.1))]).unwrap();
    assert_eq!(
        evaluate(&mut f).unwrap(),
        Field::Float(OrderedFloat(f64::from(0.1f32)))
    );
    for value in [f64::MAX, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let mut f = udf(module, vec![Field::Float(OrderedFloat(value))]).unwrap();
        assert!(matches!(
            evaluate(&mut f),
            Err(ExpressionError::Wasm(Error::Argument { .. }))
        ));
    }
    for value in ["nan", "inf", "-inf"] {
        let mut f = udf(
            &format!("(module (func (export \"f\") (result f64) f64.const {value}))"),
            vec![],
        )
        .unwrap();
        assert!(matches!(
            evaluate(&mut f),
            Err(ExpressionError::Wasm(Error::NonfiniteResult(_)))
        ));
    }
}

#[test]
fn null_propagates_without_invoking_trapping_function() {
    let mut f = udf(
        "(module (func (export \"f\") (param i32) (result i32) unreachable))",
        vec![Field::Null],
    )
    .unwrap();
    assert!(f.get_type().nullable);
    assert_eq!(evaluate(&mut f).unwrap(), Field::Null);
}

#[test]
fn invalid_arity_and_sql_types_fail_during_planning() {
    let module = "(module (func (export \"f\") (param i32) (result i32) local.get 0))";
    for args in [vec![], vec![Field::Int(1), Field::Int(2)]] {
        assert!(matches!(
            udf(module, args),
            Err(ExpressionError::InvalidNumberOfArguments { .. })
        ));
    }
    for field in [
        Field::String("hello".into()),
        Field::Float(OrderedFloat(1.0)),
        Field::Binary(vec![]),
    ] {
        assert!(matches!(
            udf(module, vec![field]),
            Err(ExpressionError::Wasm(Error::Argument { .. }))
        ));
    }
    assert!(matches!(
        udf(
            "(module (func (export \"f\") (param f64) (result f64) local.get 0))",
            vec![Field::Int(1)]
        ),
        Err(ExpressionError::Wasm(Error::Argument { .. }))
    ));
}

#[test]
fn unsupported_export_signatures_fail_during_planning() {
    for module in [
        "(module (func (export \"f\")))",
        "(module (func (export \"f\") (result i32 i32) i32.const 1 i32.const 2))",
        "(module (func (export \"f\") (result externref) ref.null extern))",
        "(module (func (export \"f\") (param funcref) (result i32) i32.const 0))",
    ] {
        assert!(matches!(
            udf(module, vec![]),
            Err(ExpressionError::Wasm(Error::Signature(_)))
        ));
    }
    for module in ["(module)", "(module (memory (export \"f\") 1))"] {
        assert!(matches!(
            udf(module, vec![]),
            Err(ExpressionError::Wasm(Error::MissingExport(_)))
        ));
    }
}

#[test]
fn missing_malformed_oversized_and_non_file_modules() {
    let dir = tempfile::tempdir().unwrap();
    let missing = WasmConfig {
        path: dir.path().join("missing").to_str().unwrap().into(),
    };
    assert!(matches!(
        Udf::new("f".into(), &missing, vec![], &Schema::default()),
        Err(ExpressionError::Wasm(Error::Read { .. }))
    ));
    for bytes in [&b"not a module"[..], &b"\0asm\x01\0\0\0\x01"[..]] {
        assert!(matches!(
            load(bytes, vec![]),
            Err(ExpressionError::Wasm(Error::Module(_)))
        ));
    }
    let path = dir.path().join("large.wasm");
    File::create(&path)
        .unwrap()
        .set_len(MAX_MODULE_BYTES as u64 + 1)
        .unwrap();
    let config = WasmConfig {
        path: path.to_str().unwrap().into(),
    };
    assert!(matches!(
        Udf::new("f".into(), &config, vec![], &Schema::default()),
        Err(ExpressionError::Wasm(Error::Config(_)))
    ));
    let path = dir.path().join("directory.wasm");
    std::fs::create_dir(&path).unwrap();
    let config = WasmConfig {
        path: path.to_str().unwrap().into(),
    };
    assert!(matches!(
        Udf::new("f".into(), &config, vec![], &Schema::default()),
        Err(ExpressionError::Wasm(Error::Config(_)))
    ));
}

#[test]
fn only_exact_assemblyscript_abort_import_is_allowed() {
    for import in [
        "(import \"wasi_snapshot_preview1\" \"fd_write\" (func))",
        "(import \"env\" \"fetch\" (func))",
        "(import \"env\" \"memory\" (memory 1))",
        "(import \"env\" \"abort\" (func (param i32)))",
    ] {
        assert!(matches!(
            udf(
                &format!("(module {import} (func (export \"f\") (result i32) i32.const 0))"),
                vec![]
            ),
            Err(ExpressionError::Wasm(Error::Import(_, _)))
        ));
    }
    let mut f = udf("(module (import \"env\" \"abort\" (func $abort (param i32 i32 i32 i32))) (func (export \"f\") (result i32) i32.const -1 i32.const -1 i32.const 0 i32.const 0 call $abort i32.const 0))", vec![]).unwrap();
    let error = evaluate(&mut f).unwrap_err();
    assert!(error.to_string().contains("AssemblyScript abort"));
}

fn assert_trap(result: Result<Field, ExpressionError>, expected: wasmi::core::TrapCode) {
    match result {
        Err(ExpressionError::Wasm(Error::Runtime { source, .. })) => {
            assert_eq!(source.as_trap_code(), Some(expected))
        }
        other => panic!("expected trap, got {other:?}"),
    }
}

#[test]
fn traps_are_errors_and_do_not_poison_later_calls() {
    let mut f = udf("(module (func (export \"f\") (param i32) (result i32) local.get 0 i32.eqz if unreachable end local.get 0))", vec![Field::Int(0)]).unwrap();
    assert_trap(
        evaluate(&mut f),
        wasmi::core::TrapCode::UnreachableCodeReached,
    );
    f.args[0] = Expression::Literal(Field::Int(7));
    assert_eq!(evaluate(&mut f).unwrap(), Field::Int(7));
}

#[test]
fn execution_fuel_and_recursion_are_bounded() {
    let mut looping = udf(
        "(module (func (export \"f\") (result i32) (loop br 0) i32.const 0))",
        vec![],
    )
    .unwrap();
    assert_trap(evaluate(&mut looping), wasmi::core::TrapCode::OutOfFuel);
    let mut recursive = udf(
        "(module (func $f (export \"f\") (result i32) call $f i32.const 1 i32.add))",
        vec![],
    )
    .unwrap();
    assert_trap(
        evaluate(&mut recursive),
        wasmi::core::TrapCode::StackOverflow,
    );
}

#[test]
fn start_functions_share_limits_and_fail_at_planning() {
    for (body, trap) in [
        ("unreachable", wasmi::core::TrapCode::UnreachableCodeReached),
        ("(loop br 0)", wasmi::core::TrapCode::OutOfFuel),
    ] {
        let result = udf(&format!("(module (func $start {body}) (start $start) (func (export \"f\") (result i32) i32.const 0))"), vec![]);
        match result {
            Err(ExpressionError::Wasm(Error::Runtime { source, .. })) => {
                assert_eq!(source.as_trap_code(), Some(trap))
            }
            other => panic!("expected start trap: {other:?}"),
        }
    }
}

#[test]
fn initial_memory_and_table_allocations_are_bounded() {
    for allocation in [
        "(memory 257)",
        "(table 4097 funcref)",
        "(table 1 funcref) (table 1 funcref)",
    ] {
        assert!(matches!(
            udf(
                &format!("(module {allocation} (func (export \"f\") (result i32) i32.const 0))"),
                vec![]
            ),
            Err(ExpressionError::Wasm(Error::Runtime { .. }))
        ));
    }
}

#[test]
fn memory_and_table_growth_are_bounded() {
    for module in [
        "(module (memory 1) (func (export \"f\") (result i32) i32.const 256 memory.grow))",
        "(module (table 1 funcref) (func (export \"f\") (result i32) ref.null func i32.const 4096 table.grow))",
    ] {
        let mut f = udf(module, vec![]).unwrap();
        assert!(matches!(evaluate(&mut f), Err(ExpressionError::Wasm(Error::Runtime { .. }))));
    }
}

#[test]
fn calls_and_clones_do_not_share_guest_state() {
    let mut f = udf("(module (global $counter (mut i32) (i32.const 0)) (func (export \"f\") (result i32) global.get $counter i32.const 1 i32.add global.set $counter global.get $counter))", vec![]).unwrap();
    let mut clone = f.clone();
    for _ in 0..3 {
        assert_eq!(evaluate(&mut f).unwrap(), Field::Int(1));
        assert_eq!(evaluate(&mut clone).unwrap(), Field::Int(1));
    }
}

#[test]
fn bounded_memory_and_table_access_succeed_but_out_of_bounds_traps() {
    let mut f = udf(
        "(module (memory 1) (table 1 funcref) (func (export \"f\") (result i32) i32.const 1 memory.grow drop ref.null func i32.const 1 table.grow drop i32.const 65536 i32.const 42 i32.store i32.const 65536 i32.load))",
        vec![],
    ).unwrap();
    assert_eq!(evaluate(&mut f).unwrap(), Field::Int(42));
    let mut invalid = udf(
        "(module (memory 1) (func (export \"f\") (result i32) i32.const 65536 i32.load))",
        vec![],
    )
    .unwrap();
    assert_trap(
        evaluate(&mut invalid),
        wasmi::core::TrapCode::MemoryOutOfBounds,
    );
}

#[test]
fn compilation_limits_reject_excess_globals() {
    let globals = "(global i32 (i32.const 0))".repeat(1001);
    assert!(matches!(
        udf(
            &format!("(module {globals} (func (export \"f\") (result i32) i32.const 0))"),
            vec![]
        ),
        Err(ExpressionError::Wasm(Error::Module(_)))
    ));
}
