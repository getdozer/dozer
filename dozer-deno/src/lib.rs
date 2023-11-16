pub use deno_runtime;

mod ts_module_loader;
pub use ts_module_loader::TypescriptModuleLoader;

mod runtime;
pub use runtime::{Error as RuntimeError, Runtime};

fn user_agent() -> String {
    let version: String = env!("CARGO_PKG_VERSION").into();
    format!(
        "Dozer/{} {}",
        version,
        deno_runtime::BootstrapOptions::default().user_agent
    )
}
