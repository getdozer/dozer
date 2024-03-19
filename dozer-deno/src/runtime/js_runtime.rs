use std::rc::Rc;

use crate::runtime::permissions::PermissionsContainer;
use deno_broadcast_channel::InMemoryBroadcastChannel;
use deno_cache::SqliteBackedCache;
use deno_console::deno_console;
use deno_core::{
    error::AnyError, extension, Extension, JsRuntime, ModuleId, ModuleSpecifier, RuntimeOptions,
};
use deno_crypto::deno_crypto;
use deno_napi::deno_napi;
use deno_tls::deno_tls;
use deno_url::deno_url;
use deno_web::deno_web;
use deno_webidl::deno_webidl;
use deno_websocket::deno_websocket;
use deno_webstorage::deno_webstorage;
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
        "98_global_scope.js",
        "99_main.js",
    ],
);

pub struct JsWorker {
    pub js_runtime: JsRuntime,
}

impl JsWorker {
    /// This is `MainWorker::from_options` with selected list of extensions.
    pub fn new(extra_extensions: Vec<Extension>) -> Result<Self, std::io::Error> {
        let mut extensions = {
            vec![
                deno_webidl::init_ops_and_esm(),
                deno_console::init_ops_and_esm(),
                deno_url::init_ops_and_esm(),
                deno_web::init_ops_and_esm::<PermissionsContainer>(
                    Default::default(),
                    Default::default(),
                ),
                deno_fetch::deno_fetch::init_ops_and_esm::<PermissionsContainer>(
                    deno_fetch::Options {
                        file_fetch_handler: Rc::new(deno_fetch::FsFetchHandler),
                        ..Default::default()
                    },
                ),
                deno_cache::deno_cache::init_ops_and_esm::<SqliteBackedCache>(Default::default()),
                deno_websocket::init_ops_and_esm::<PermissionsContainer>(
                    user_agent(),
                    Default::default(),
                    Default::default(),
                ),
                deno_webstorage::init_ops_and_esm(Default::default()),
                deno_crypto::init_ops_and_esm(Default::default()),
                deno_broadcast_channel::deno_broadcast_channel::init_ops_and_esm::<
                    InMemoryBroadcastChannel,
                >(Default::default()),
                deno_tls::init_ops_and_esm(),
                deno_napi::init_ops_and_esm::<PermissionsContainer>(),
                //ops::bootstrap::deno_bootstrap::init_ops_and_esm(Some(Default::default())),
                dozer_permissions_worker::init_ops_and_esm(PermissionsContainer::allow_all()),
                runtime::init_ops_and_esm(),
            ]
        };
        extensions.extend(extra_extensions);

        Ok(JsWorker {
            js_runtime: JsRuntime::new(RuntimeOptions {
                module_loader: Some(Rc::new(TypescriptModuleLoader::new()?)),
                extensions,
                ..Default::default()
            }),
        })
    }

    /// `MainWorker::evaluate_module`.
    pub async fn evaluate_module(&mut self, id: ModuleId) -> Result<(), AnyError> {
        let mut receiver = self.js_runtime.mod_evaluate(id);
        select! {
            biased;

            maybe_result = &mut receiver => {
                maybe_result
            }

            event_loop_result = self.js_runtime.run_event_loop(Default::default()) => {
                event_loop_result?;
                receiver.await
            }
        }
    }

    pub async fn execute_main_module(
        &mut self,
        module_specifier: &ModuleSpecifier,
    ) -> Result<(), AnyError> {
        let id = self.preload_main_module(module_specifier).await?;
        self.evaluate_module(id).await
    }

    pub async fn preload_main_module(
        &mut self,
        module_specifier: &ModuleSpecifier,
    ) -> Result<ModuleId, AnyError> {
        self.js_runtime.load_main_es_module(module_specifier).await
    }
    pub async fn preload_side_module(
        &mut self,
        module_specifier: &ModuleSpecifier,
    ) -> Result<ModuleId, AnyError> {
        self.js_runtime.load_side_es_module(module_specifier).await
    }
}
