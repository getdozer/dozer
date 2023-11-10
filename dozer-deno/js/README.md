# Bootstrap development notes

Files in this directory are adapted from `deno_runtime/js/`. The best way to view the original files is to compile this project and jump to`deno_runtime` crate source.

To expose additional APIs to the JavaScript runtime, `dozer-deno/src/runtime/js_runtime.rs` and these files should be updated together.

When updating to a new `deno_runtime` version, these files should be updated accordingly. The functions in `dozer-deno/src/runtime/js_runtime.rs` should also be revised to keep in sync with original code in `deno_runtime`.
