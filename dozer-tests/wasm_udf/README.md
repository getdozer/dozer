# WebAssembly scalar UDFs

Build Dozer with `--features wasm` to enable scalar WebAssembly functions. Configure each function in the existing `udfs` list:

```yaml
udfs:
  - name: increment
    config: !Wasm
      path: ./math.wasm
      function: addOne
```

`name` is the SQL function name; use lowercase names, as Dozer normalizes SQL function names to lowercase. `function` is the case-sensitive module export and defaults to `name`. `path` is resolved from the process working directory. Call it like any other scalar function: `SELECT increment(value) FROM input`.

## AssemblyScript example

From the repository root, with Node.js 16+ and the normal Rust/protobuf build prerequisites installed:

```sh
npm ci --prefix dozer-tests/wasm_udf
npm run build --prefix dozer-tests/wasm_udf
cargo run -p dozer-sql-expression --features wasm --example wasm_udf -- dozer-tests/wasm_udf/dozer-config.yaml
```

This compiles `assembly/index.ts` into a real `.wasm` module, reads the example YAML as a Dozer configuration, parses its SQL with the Dozer dialect, and evaluates the expressions on three in-memory rows with nullable integer `value` and floating-point `amount` columns. No connector or database is required for this example. The row `(41, 41.0)` produces `42`, `41.5`, and `41`; a null row produces three nulls; `(-1, -1.0)` demonstrates an AssemblyScript assertion reported as a SQL evaluation error.

To use the same UDFs in an ingestion pipeline, add the `udfs` entries and SQL to the pipeline's normal configuration, then run a CLI built with `cargo build -p dozer-cli --features wasm`.

## Supported ABI and execution

Each exported function must return exactly one `i32`, `i64`, `f32`, or `f64` value. Integer parameters accept Dozer integer fields when the value fits the signed WebAssembly parameter width, and booleans as `0`/`1`. Floating-point parameters accept Dozer `Float`; use an explicit SQL `CAST(... AS FLOAT)` for integer inputs. Integer results become Dozer `Int`, floating-point results become `Float`. An `f32` parameter uses normal IEEE rounding; finite values that overflow are rejected.

Any null argument returns SQL null without invoking the exported function. Wrong argument counts, unsupported types, missing exports, invalid modules, unsupported imports, and failing module start functions are rejected during SQL planning. Module paths are read and compiled during planning; evaluation reuses that compiled module even if the file changes later. Each row receives a fresh instance so mutable globals and memory do not leak between records.

Execution allows up to 10 million fuel units per instantiation/call, one memory of up to 64 MiB, and one table of up to 10,000 elements. Traps and fuel exhaustion become SQL errors. The only host import is AssemblyScript's `env.abort`, which reports the source line and column. WASI, host I/O, strings/objects passed through linear memory, multiple results, and reference types are outside this scalar ABI.

Without the `wasm` feature, configuration still deserializes and calling a configured WASM UDF returns a clear feature-disabled error.

## Tests

```sh
cargo test -p dozer-sql-expression --features wasm
cargo test -p dozer-sql-expression --no-default-features --test wasm_udf
cargo test -p dozer-types udf_yaml_deserialize
```
