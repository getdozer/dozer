use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);
impl Fixture {
    fn new(wat: &str) -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "dozer-wasm-{}-{}.wasm",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, wat::parse_str(wat).unwrap()).unwrap();
        Self(path)
    }
    fn config(&self) -> WasmConfig {
        WasmConfig::new(self.0.to_str().unwrap().into())
    }
    fn udf(&self, fields: Vec<Field>) -> Result<Udf, Error> {
        Udf::new(
            "f".into(),
            &self.config(),
            fields.into_iter().map(Expression::Literal).collect(),
            &Schema::default(),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn eval(udf: &mut Udf) -> Result<Field, Error> {
    udf.evaluate(&Record::new(vec![]), &Schema::default())
}

#[test]
fn numeric_abi_checks_signed_and_unsigned_boundaries() {
    for (wasm_type, minimum, maximum) in [
        ("i32", i32::MIN as i64, i32::MAX as i64),
        ("i64", i64::MIN, i64::MAX),
    ] {
        let fixture = Fixture::new(&format!(
            "(module (func (export \"f\") (param {wasm_type}) (result {wasm_type}) local.get 0))"
        ));
        for field in [
            Field::Int(minimum),
            Field::Int(maximum),
            Field::UInt(maximum as u64),
            Field::Int8(-2),
            Field::Boolean(true),
            Field::Boolean(false),
        ] {
            let expected = match field {
                Field::UInt(v) => v as i64,
                Field::Int(v) => v,
                Field::Int8(v) => i64::from(v),
                Field::Boolean(v) => i64::from(v),
                _ => unreachable!(),
            };
            assert_eq!(
                eval(&mut fixture.udf(vec![field]).unwrap()).unwrap(),
                Field::Int(expected)
            );
        }
        for field in [
            Field::I128(i128::from(minimum) - 1),
            Field::U128(maximum as u128 + 1),
            Field::UInt(u64::MAX),
            Field::U128(u128::MAX),
        ] {
            assert!(eval(&mut fixture.udf(vec![field]).unwrap())
                .unwrap_err()
                .to_string()
                .contains("in-range"));
        }
        assert!(fixture.udf(vec![Field::Float(1.0.into())]).is_err());
    }
}

#[test]
fn floating_abi_rejects_nonfinite_values_and_f32_overflow() {
    for ty in ["f32", "f64"] {
        let fixture = Fixture::new(&format!(
            "(module (func (export \"f\") (param {ty}) (result {ty}) local.get 0))"
        ));
        assert_eq!(
            eval(&mut fixture.udf(vec![Field::Float((-1.25).into())]).unwrap()).unwrap(),
            Field::Float((-1.25).into())
        );
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(eval(&mut fixture.udf(vec![Field::Float(value.into())]).unwrap()).is_err());
        }
        if ty == "f32" {
            assert!(eval(&mut fixture.udf(vec![Field::Float(f64::MAX.into())]).unwrap()).is_err());
        }
        assert!(fixture.udf(vec![Field::Int(1)]).is_err());
        for value in ["nan", "inf", "-inf"] {
            let output = Fixture::new(&format!(
                "(module (func (export \"f\") (result {ty}) {ty}.const {value}))"
            ));
            assert!(eval(&mut output.udf(vec![]).unwrap())
                .unwrap_err()
                .to_string()
                .contains("NaN or infinity"));
        }
    }
}

#[test]
fn null_is_propagated_without_running_the_function() {
    let fixture =
        Fixture::new("(module (func (export \"f\") (param i32) (result i32) unreachable))");
    let mut udf = fixture.udf(vec![Field::Null]).unwrap();
    assert!(udf.get_type().nullable);
    assert_eq!(eval(&mut udf).unwrap(), Field::Null);
}

#[test]
fn module_validation_rejects_imports_exports_and_unsupported_signatures() {
    for wasm in [
        "(module)",
        "(module (memory (export \"f\") 1))",
        "(module (func (export \"f\")))",
        "(module (func (export \"f\") (result i32 i32) i32.const 1 i32.const 2))",
        "(module (func (export \"f\") (param externref) (result i32) i32.const 1))",
        "(module (import \"env\" \"seed\" (func)) (func (export \"f\") (result i32) i32.const 1))",
        "(module (import \"env\" \"abort\" (func (param i32))) (func (export \"f\") (result i32) i32.const 1))",
        "(module (import \"env\" \"memory\" (memory 1)) (func (export \"f\") (result i32) i32.const 1))",
    ] {
        assert!(Fixture::new(wasm).udf(vec![]).is_err(), "accepted: {wasm}");
    }
    let fixture =
        Fixture::new("(module (func (export \"f\") (param i32) (result i32) local.get 0))");
    assert!(fixture.udf(vec![]).is_err());
    assert!(fixture
        .udf(vec![Field::String("not a pointer".into())])
        .is_err());
}

#[test]
fn traps_fuel_and_memory_growth_are_bounded() {
    for wasm in [
        "(module (func (export \"f\") (result i32) unreachable))",
        "(module (func (export \"f\") (result i32) (loop br 0) i32.const 1))",
        "(module (memory 1) (func (export \"f\") (result i32) i32.const 1000 memory.grow))",
        "(module (table 1 funcref) (func (export \"f\") (result i32) ref.null func i32.const 5000 table.grow))",
        "(module (import \"env\" \"abort\" (func $abort (param i32 i32 i32 i32))) (func (export \"f\") (result i32) i32.const 0 i32.const 0 i32.const 0 i32.const 0 call $abort i32.const 1))",
    ] {
        let fixture = Fixture::new(wasm);
        let mut config = fixture.config();
        config.fuel = 1000;
        let mut udf = Udf::new("f".into(), &config, vec![], &Schema::default()).unwrap();
        assert!(eval(&mut udf).is_err(), "accepted: {wasm}");
        assert!(eval(&mut udf).is_err(), "a failed invocation leaked state");
    }
}

#[test]
fn startup_is_metered_and_memory_limits_apply_before_instantiation() {
    for wasm in [
        "(module (func $start (loop br 0)) (start $start) (func (export \"f\") (result i32) i32.const 1))",
        "(module (func $start unreachable) (start $start) (func (export \"f\") (result i32) i32.const 1))",
        "(module (memory 1000) (func (export \"f\") (result i32) i32.const 1))",
        "(module (table 5000 funcref) (func (export \"f\") (result i32) i32.const 1))",
    ] {
        let fixture = Fixture::new(wasm);
        let mut config = fixture.config();
        config.fuel = 1000;
        assert!(Compiled::load("f", &config).is_err());
    }
}

#[test]
fn every_record_gets_fresh_globals_and_memory() {
    let fixture = Fixture::new(
        "(module (global $counter (mut i64) (i64.const 0)) (memory 1)
      (func (export \"f\") (result i64)
        global.get $counter i64.const 1 i64.add global.set $counter
        i32.const 0 i32.const 0 i64.load i64.const 10 i64.add i64.store
        i32.const 0 i64.load global.get $counter i64.add))",
    );
    let mut udf = fixture.udf(vec![]).unwrap();
    for _ in 0..3 {
        assert_eq!(eval(&mut udf).unwrap(), Field::Int(11));
    }
    assert_eq!(eval(&mut udf.clone()).unwrap(), Field::Int(11));
}

#[test]
fn wasm_integer_arithmetic_retains_its_defined_wrapping_semantics() {
    let fixture = Fixture::new(
        "(module (func (export \"f\") (param i32) (result i32) local.get 0 i32.const 1 i32.add))",
    );
    assert_eq!(
        eval(&mut fixture.udf(vec![Field::Int(i32::MAX.into())]).unwrap()).unwrap(),
        Field::Int(i32::MIN.into())
    );
}

#[test]
fn invalid_limits_missing_files_and_duplicate_names_fail_startup() {
    let fixture = Fixture::new("(module (func (export \"f\") (result i32) i32.const 1))");
    for fuel in [0, MAX_FUEL + 1] {
        let mut config = fixture.config();
        config.fuel = fuel;
        assert!(Compiled::load("f", &config).is_err());
    }
    for memory_limit in [0, MAX_MEMORY + 1] {
        let mut config = fixture.config();
        config.memory_limit = memory_limit;
        assert!(Compiled::load("f", &config).is_err());
    }
    let config = UdfConfig {
        name: "f".into(),
        config: UdfType::Wasm(fixture.config()),
    };
    assert!(validate_udfs(&[config.clone(), config]).is_err());
    assert!(Compiled::load(
        "f",
        &WasmConfig::new(fixture.0.with_extension("missing").to_str().unwrap().into())
    )
    .is_err());
    std::fs::write(&fixture.0, b"not wasm").unwrap();
    assert!(fixture.udf(vec![]).is_err());
    File::create(&fixture.0)
        .unwrap()
        .set_len(MAX_MODULE_BYTES + 1)
        .unwrap();
    assert!(fixture
        .udf(vec![])
        .unwrap_err()
        .to_string()
        .contains("10 MiB"));
}
