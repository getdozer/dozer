mod ts_module_loader;
pub use ts_module_loader::TypescriptModuleLoader;

mod runtime;
pub use runtime::{Error as RuntimeError, JsWorker, Runtime};

fn user_agent() -> String {
    let version: String = env!("CARGO_PKG_VERSION").into();
    format!("Dozer/{} {}", version, deno_core::v8_version())
}
