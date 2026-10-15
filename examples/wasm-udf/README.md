# WebAssembly scalar UDFs

Compile an AssemblyScript function, register the `.wasm` file, then call it from
SQL. Dozer executes the binary with wasmi; Node and JavaScript bindings are not
part of the execution path.

## Build and configure

From this example directory:

```sh
npm ci --ignore-scripts
npm run build
```

The pinned AssemblyScript compiler produces `udfs/wasm/my_function.wasm` from
`assembly/index.ts`. The binary and readable `.wat` are included so Rust tests
do not need Node or download a compiler.

Add this to the project's `dozer-config.yaml`:

```yaml
udfs:
  wasm:
    my_function: udfs/wasm/my_function
```

Run Dozer from the project directory containing `udfs/`. Paths use Dozer's
existing **working directory** convention, including when `--config` points
elsewhere; they are not relative to each configuration file. Absolute paths are
also accepted. A path without an extension receives `.wasm` automatically.
Configure a source named `input_rows` with integer fields `field1` and `field2`,
then use:

```sql
SELECT my_function(field1, field2) INTO result FROM input_rows;
```

This example's configuration does not invent a live connector. The integration
test supplies synthetic source operations to the actual SQL planner and processor
factories.

The existing list format also works, and supports an export alias and limits:

```yaml
udfs:
  - name: scaled
    config: !Wasm
      path: udfs/wasm/my_function.wasm
      export: scale
      fuel: 1000000
      memory_limit: 16777216
```

SQL names are case-insensitive; WASM export names are case-sensitive and default
to the configured name. Export one fixed-arity function for each registered UDF.
Calls take positional scalar expressions; named arguments, wildcards, window or
aggregate modifiers, and SQL return-type overrides are rejected.
Do not compile with `--exportStart`: use the compiler's normal implicit module
initialization. AssemblyScript optional parameters and object bindings are not
part of this ABI.

## Values and isolation

| WASM type | SQL input | SQL result |
| --- | --- | --- |
| `i32` | Integer fields within the signed 32-bit range | `Int` |
| `i64` | Integer fields within the signed 64-bit range | `Int` |
| `f32` | `Float`, rounded to finite single precision | `Float` |
| `f64` | Finite `Float` | `Float` |

Use an explicit SQL `CAST(... AS FLOAT)` when passing an integer expression to a
floating-point parameter. Unsigned and 128-bit integer fields are checked before
conversion. NaN, infinities and out-of-range arguments are errors. A NULL argument
returns SQL NULL without invoking the function; output nullability follows the
arguments (computed expressions are conservatively nullable). Each function must
return exactly one numeric value. Strings, objects,
pointers, references and multiple return values are unsupported.

WASM arithmetic keeps its standard semantics: integer addition can wrap inside
the guest. Host argument conversions never wrap; use AssemblyScript checks or
assertions when arithmetic overflow should fail. Division traps and assertions
are reported as SQL expression errors.

Modules are validated during pipeline setup, including exports, parameter counts,
types, imports and initialization. A compiled module is retained by each SQL
expression, while every evaluation gets fresh memory, globals, fuel and limits.
An update/delete therefore recomputes the same function as the original insert;
mutable guest state cannot accumulate across records. The only allowed import is
AssemblyScript's `env.abort(i32, i32, i32, i32)`, which raises an error without
reading guest pointers. Filesystem, network, clocks, random sources and WASI imports
are unavailable.

The default per-evaluation budget is 1,000,000 fuel units and 16 MiB of linear
memory, including initialization. Configurations may use at most 10,000,000 fuel
and 64 MiB. Modules are at most 10 MiB; one memory, one table and 4,096 table
elements are allowed. Failed memory/table growth traps. Fuel exhaustion and guest
traps return errors; a later record receives a new instance and can still run.
Fuel bounds execution, not compilation time. Fresh instantiation prioritizes
repeatable streaming results over the throughput of a stateful instance pool.

## Reproduce the local verification

From the repository root, with its normal Rust/protoc environment:

```sh
cargo test --locked -p dozer-sql-expression --no-default-features --lib wasm::tests
cargo test --locked -p dozer-sql --no-default-features --lib tests::wasm_udf
cargo test --locked -p dozer-sql --no-default-features --lib \
  tests::wasm_udf::wasm_assemblyscript_streaming_demo -- --exact --nocapture
```

The demo uses the checked-in AssemblyScript binary, original mapping configuration,
real SQL schema propagation and processor factories. It checks insert, update,
delete and NULL events. A synchronous queue replaces connector transport; it does
not demonstrate a live connector or checkpoint recovery.

References: [AssemblyScript compiler](https://www.assemblyscript.org/compiler.html),
[AssemblyScript imports](https://www.assemblyscript.org/concepts.html#special-imports),
[wasmi 0.31.2](https://docs.rs/wasmi/0.31.2/wasmi/).
