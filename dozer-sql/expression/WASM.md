# Numeric WASM UDFs

Register a function using the current Dozer UDF configuration format:

```yaml
udfs:
  - name: calculate
    config: !Wasm
      path: ./udfs/numeric.wasm
      export: add
```

`export` is optional and defaults to `name`. Paths resolve from the process working
directory, just like existing file-based UDF configuration. A missing file, invalid
module, import, missing export, unsupported signature, or SQL type mismatch fails
expression construction. The old issue's `udfs.wasm` map is not the current config
schema; this implementation uses the same tagged configuration as Onnx/JavaScript.

Compile the checked-in AssemblyScript fixture with AssemblyScript 0.27.31:

```sh
npm exec --yes --package assemblyscript@0.27.31 -- asc \
  dozer-sql/expression/tests/fixtures/numeric.ts \
  --runtime stub --noAssert --optimize \
  --outFile dozer-sql/expression/tests/fixtures/numeric.wasm
```

The generated binary is checked in so Rust tests do not require Node or npm.
The fixture exports `add(i64, i64) -> i64` and `twice(f64) -> f64`. For example,
`SELECT calculate(integer_column, 22) FROM input` invokes `add` with the config
above. The SQL-builder regression invokes both actual compiled exports.

## ABI and isolation

- One numeric result: i32/i64 maps to SQL Int; f32/f64 maps to SQL Float.
- Integer parameters require SQL Int; floating parameters require SQL Float.
  Use SQL casts where necessary. i32 overflow is an error, not truncation.
- NULL input propagates to NULL without executing the module.
- No imports: no WASI, network, filesystem, host callbacks, or imported memory.
  Compile numeric AssemblyScript without abort imports. Strings, arrays, objects,
  references, multiple results, and managed-runtime ABIs are not supported.
- Each row gets a fresh instance and independent memory/globals. State does not
  survive evaluation or leak between cloned expressions. This favors predictable
  behavior over stateful/high-throughput module reuse.
- Modules are at most 8 MiB; each evaluation has 1,000,000 fuel, at most 16 MiB
  linear memory and 1024 table elements, one instance, one memory and one table.
  Start functions share the fuel budget. Traps and exhausted limits are errors.
- NaN/infinity arguments and results are rejected. f32 conversion uses normal
  IEEE rounding, and rejects overflow to infinity.

Only load administrator-selected modules. Execution budgets do not constitute a
general guarantee against untrusted compiler/runtime vulnerabilities.

## Tests

```sh
cargo test -p dozer-sql-expression --lib wasm::tests --locked
cargo test -p dozer-types --lib udf_yaml_deserialize --locked
cargo test -p dozer-sql --lib --locked
```

Current coverage includes real compiled AssemblyScript via the SQL expression
builder, configuration parsing, NULLs, i32 overflow, malformed modules, signature
mismatch, imports, excessive initial memory, and infinite-loop fuel exhaustion.
The SQL package also runs `test_wasm_source_to_sink_pipeline`: the actual
`statement_to_pipeline` and `DagExecutor` load the compiled module and forward ten
source records through a SQL WASM projection into a collecting sink. All ten
results are asserted as Int(42). This is not a standalone CLI deployment or a
validation of all optional feature combinations.
