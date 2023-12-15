use std::rc::Rc;

use deno_runtime::{
    deno_broadcast_channel::{deno_broadcast_channel, InMemoryBroadcastChannel},
    deno_cache::{deno_cache, SqliteBackedCache},
    deno_console::deno_console,
    deno_core::{error::AnyError, extension, Extension, JsRuntime, ModuleId, RuntimeOptions},
    deno_crypto::deno_crypto,
    deno_fetch::deno_fetch,
    deno_napi::deno_napi,
    deno_tls::deno_tls,
    deno_url::deno_url,
    deno_web::deno_web,
    deno_webidl::deno_webidl,
    deno_websocket::deno_websocket,
    deno_webstorage::deno_webstorage,
    permissions::PermissionsContainer,
};
use tokio::select;

use crate::{user_agent, TypescriptModuleLoader};

extension!(
    dozer_permissions_worker,
    options = {
        permissions: PermissionsContainer,
    },
    state = |state, options| {
        state.put(options.permissions)
    }
);

extension!(
    runtime,
    deps = [
        deno_webidl,
        deno_console,
        deno_url,
        deno_tls,
        deno_web,
        deno_fetch,
        deno_cache,
        deno_websocket,
        deno_webstorage,
        deno_crypto,
        deno_broadcast_channel,
        deno_napi
    ],
    esm_entry_point = "ext:runtime/99_main.js",
    esm = [
        dir "js",
        "06_util.js",
        "98_global_scope.js",
        "99_main.js",
    ],
);

/// This is `MainWorker::from_options` with selected list of extensions.
pub fn new(extra_extensions: Vec<Extension>) -> Result<JsRuntime, std::io::Error> {
    let mut extensions = {
        vec![
            deno_webidl::init_ops_and_esm(),
            deno_console::init_ops_and_esm(),
            deno_url::init_ops_and_esm(),
            deno_web::init_ops_and_esm::<PermissionsContainer>(
                Default::default(),
                Default::default(),
            ),
            deno_fetch::init_ops_and_esm::<PermissionsContainer>(Default::default()),
            deno_cache::init_ops_and_esm::<SqliteBackedCache>(Default::default()),
            deno_websocket::init_ops_and_esm::<PermissionsContainer>(
                user_agent(),
                Default::default(),
                Default::default(),
            ),
            deno_webstorage::init_ops_and_esm(Default::default()),
            deno_crypto::init_ops_and_esm(Default::default()),
            deno_broadcast_channel::init_ops_and_esm::<InMemoryBroadcastChannel>(Default::default()),
            deno_tls::init_ops_and_esm(),
            deno_napi::init_ops_and_esm::<PermissionsContainer>(),
            dozer_permissions_worker::init_ops_and_esm(PermissionsContainer::allow_all()),
            runtime::init_ops_and_esm(),
        ]
    };
    extensions.extend(extra_extensions);

    Ok(JsRuntime::new(RuntimeOptions {
        module_loader: Some(Rc::new(TypescriptModuleLoader::new()?)),
        extensions,
        ..Default::default()
    }))
}

/// `MainWorker::evaluate_module`.
pub async fn evaluate_module(runtime: &mut JsRuntime, id: ModuleId) -> Result<(), AnyError> {
    let mut receiver = runtime.mod_evaluate(id);
    select! {
        biased;

        maybe_result = &mut receiver => {
            maybe_result.expect("Module evaluation result not provided.")
        }

        event_loop_result = runtime.run_event_loop(false) => {
            event_loop_result?;
            let maybe_result = receiver.await;
            maybe_result.expect("Module evaluation result not provided.")
        }
    }
}
