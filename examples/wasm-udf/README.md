# Local WASM scalar UDFs

Build Dozer with `cargo build --locked -p dozer-cli --no-default-features --features wasm` from the repository root. This example and the locked Rust dependencies have been tested with Rust 1.79.0 and protoc 25.3. The `wasm` feature is optional and forwarded through the CLI, SQL and expression crates; no Deno or WASI runtime is enabled by it.

The checked-in `udfs/wasm/my_function.wasm` is compiled from `assembly/my_function.ts` with AssemblyScript 0.27.31. To reproduce it, from this directory run `npm ci --ignore-scripts` followed by `npm run build` (Node 16 or later). Its SHA-256 is `80345843d050cc2d3e7a012d4ce8bf715560e0a4ef724ffdca2f76872832da72`. Run `npm run build:primitives` to rebuild the additional integer, float and Boolean test module from `assembly/primitives.ts`; its SHA-256 is `931030cc463305cb09170e99e4694944330d421a9cd100a933c05c2354da3950`.

## Configuration and SQL

The example uses the configuration requested in issue #1653:

```yaml
udfs:
  wasm:
    my_function: udfs/wasm/my_function
```

Paths are relative to Dozer's working directory, consistent with the existing local UDF configuration. A path with no extension gets `.wasm` appended. An explicit `.wasm` path also works. The binary must export a function named `my_function`. Use lowercase ASCII identifiers for configured names; SQL calls are case insensitive. Names that conflict with built-in SQL functions or the Python UDF prefix are rejected. Duplicate registrations, including conflicts between UDF types, are rejected case insensitively.

The existing tagged-list format remains supported, including mixed WASM, ONNX and JavaScript entries:

```yaml
udfs:
  - name: my_function
    config: !Wasm
      path: udfs/wasm/my_function.wasm
```

An explicit `type: wasm` entry is also accepted in the same list:

```yaml
udfs:
  - type: wasm
    name: my_function
    config:
      path: udfs/wasm/my_function.wasm
```

Field order does not matter. Use either this plain `config.path` form with `type: wasm` or the tagged form above; combining the two is rejected. Unknown or duplicate fields are rejected. The type must be lowercase `wasm`. Serialization keeps Dozer's existing tagged format. Explicit WASM entries may be mixed with existing `!Onnx`, `!JavaScript` and `!Wasm` entries.

Use `SELECT my_function(a, b) AS sum INTO computed FROM numbers;`. All configured WASM exports are read, compiled and initialized before connector setup, including unused functions and configurations without SQL. Missing files, malformed modules, invalid imports/exports, unsupported signatures and failing start functions abort startup. SQL argument compatibility is checked when the input schema is available, including calls in WHERE clauses.

One application registry retains the loaded code for schema planning and execution. Cloned pipelines share immutable compiled modules; separate applications and changed UDF configurations get independent registries. Success or failure is cached for that registry, so a partially loaded set cannot become usable. Deleting or replacing a module file after startup does not change a running query. Restart Dozer to reload a file or retry failed startup. A standalone caller of `Udf::new` still loads its own module; callers constructing several expressions can pass a shared `UdfRegistry` to the planner/builder.

Run `python3 demo.py` after building Dozer. The demonstration chooses a free loopback port, uses temporary storage, sends synthetic numeric records through the actual webhook connector, checks sink output and stops its process. It adds `NOW() AS inserted_at` only for the dummy sink's existing debug output. No accounts, keys or external services are used. Expected final line:

```text
PASS (mapping configuration): local webhook -> Dozer SQL -> compiled AssemblyScript -> sink: 7, 8, NULL
```

Run `python3 demo.py --udf-format typed` to repeat the same real pipeline using `type: wasm`, or `--udf-format tagged` for `config: !Wasm`. The demo creates a temporary configuration without editing the checked-in example.

Run `python3 verify_startup.py --results /tmp/dozer-wasm-startup-results.json` to check six invalid-startup cases against the actual CLI. It asserts a nonzero exit with the expected error and no observed listener, retains every subprocess log in the result, and stops its own subprocesses. The cases cover unused missing/malformed modules, missing exports, no-SQL startup, duplicate registrations and built-in name conflicts.

For a manual run, from this directory use `../../target/debug/dozer --ignore-pipe run`, then send JSON to `http://127.0.0.1:18765/numbers`:

```json
{"numbers":[{"a":3,"b":4},{"a":3,"b":5},{"b":4}]}
```

The current webhook connector maps an omitted nullable field to SQL NULL; explicit JSON null for integer fields is rejected by that connector. It supports POST/DELETE, not PUT updates. SQL processor tests exercise Update events directly. The dummy sink discards output by default. The automated demo enables its record logging and checks the actual values. Stop a manual run with Ctrl-C.

## ABI

Wasmi 0.39.1 executes core WebAssembly modules with numeric scalar exports. Every exposed function must have exactly one result; zero or more arguments are allowed within the runtime's compilation limits.

| WASM parameter | Accepted Dozer values | Conversion |
| --- | --- | --- |
| `i32` | Int, Int8, I128, UInt, U128, Boolean | Checked signed 32-bit range; overflow is an error; Boolean becomes 0 or 1 |
| `i64` | Int, Int8, I128, UInt, U128 | Checked signed 64-bit range; overflow is an error |
| `f32` | Float | IEEE narrowing/rounding, including underflow; overflow and nonfinite input rejected |
| `f64` | Float | Finite values only, without narrowing |

Integer results become Dozer `Int`; floating results become `Float` (`f32` is widened to `f64`). Nonfinite floating results are errors. AssemblyScript Boolean inputs are supported as 0/1; a Boolean result uses the binary's `i32` result type and is therefore inferred as SQL `Int` (0/1). An explicit SQL Boolean cast can express the desired output interpretation. WASM integer signedness and source-level types are not encoded in the binary ABI: this interface interprets integer results as signed. Do not export pointers, managed strings, or unsigned values expecting a different interpretation merely because they compile to `i32`/`i64`.

Validation emits warnings when an integer schema permits values outside the target signed range or a Float is narrowed to `f32`. Values outside the accepted range fail during execution; finite `f32` rounding/underflow is allowed. Warnings occur during expression binding, not per input row. Schema propagation and processor construction may each bind an expression.

There are no implicit integer/float conversions, string marshalling, memory/pointer arguments, reference values or multiple results. Non-primitive types were explicitly deferred in the [maintainer's original review](https://github.com/getdozer/dozer/pull/1654#pullrequestreview-1616573505). SQL NULL propagates without invoking the guest. Nullable input makes the inferred output nullable. Use explicit SQL casts for numeric type conversion. Scalar calls support expressions, nesting and use within an aggregate, but aggregate expressions *inside* UDF arguments, window clauses, DISTINCT, function ORDER BY, named/wildcard arguments and explicit return-type annotations are unsupported.

WASM arithmetic inside the module follows WebAssembly rules (for example i32 addition wraps); host input range checks do not change those rules.

## Runtime controls and limits

- The only accepted import is AssemblyScript `env.abort(i32,i32,i32,i32) -> ()`, which returns a host error without reading guest pointers. No WASI or host filesystem, network, clock, randomness, environment or logging services are linked. Unexpected imports fail startup.
- A fresh store/instance is used for every non-NULL call. Mutable guest globals/memory cannot accumulate across rows or cloned expressions. This preserves repeatability when updates/deletes reevaluate old records. Compiled modules are reused, but instantiation costs occur per call.
- Execution has 1,000,000 Wasmi fuel units, shared by the start function and exported call. Startup validation also runs initialization with the same limits. Fuel is not milliseconds.
- At most one linear memory (16 MiB), one table (4096 elements), and one instance per store. Excess initial allocations and growth fail. The value stack is capped at 16,384 Wasmi slots and recursion depth at 128.
- Module reads are capped at 4 MiB and use Wasmi's strict compilation limits. Compilation is not fuel-metered. These limits do not bound total process RSS, compilation wall time, or total memory for many distinct UDF expressions.
- Guest traps, limit failures and invalid arguments become query errors, not successful NULL results. There is no OS process sandbox, runtime security audit or throughput guarantee in this change. The operator controls the module path; these controls restrict guest capabilities rather than access to paths chosen in the trusted configuration.

## Tests

From the repository root:

```sh
cargo test --locked -p dozer-sql -p dozer-sql-expression -p dozer-types -p dozer-cli --no-default-features --features dozer-cli/wasm --lib
cargo test --locked -p dozer-sql -p dozer-sql-expression -p dozer-types -p dozer-cli --no-default-features --lib
cargo test --locked -p dozer-sql --features wasm assemblyscript_streaming_projection -- --nocapture
cargo fmt --all -- --check
```

The runtime tests use generated WASM fixtures to exercise failure paths, limits, concurrent preparation, file replacement/removal, cached startup failure, separate application registries and guest-state isolation. SQL tests load the actual AssemblyScript binaries, check schema inference, inserts, updates, deletes, batches, NULL, nested calls, aggregate input, grouping/HAVING and WHERE membership transitions. They remove a prepared module before schema propagation and processor creation to verify that both phases use the same snapshot. CLI tests cover configuration changes between orchestrator clones and invalid unused UDFs before connector setup. The feature-disabled suite verifies a useful startup error without loading a guest. `demo.py` and `verify_startup.py` separately cover the built CLI and live loopback connector.
