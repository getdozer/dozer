//! `JsRuntime` is `!Send + !Sync`, make it difficult to use.
//! Here we implement a `Runtime` struct that runs `JsRuntime` in a dedicated thread.
//! By sending work to the worker thread, `Runtime` is `Send + Sync`.

use std::{collections::HashMap, fs::canonicalize, num::NonZeroI32, thread::JoinHandle};

use deno_runtime::{
    deno_core::{
        anyhow::{bail, Context as _},
        error::AnyError,
        Extension, JsRuntime, ModuleSpecifier,
    },
    deno_napi::v8::{self, undefined, Function, Global, Local},
};
use dozer_types::{
    json_types::JsonValue,
    log::{error, info},
    thiserror,
};
use tokio::sync::{mpsc, oneshot};

use self::conversion::{from_v8, to_v8};

#[derive(Debug)]
pub struct Runtime {
    work_sender: mpsc::Sender<Work>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create JavaScript runtime: {0}")]
    CreateJsRuntime(#[source] std::io::Error),
    #[error("failed to canonicalize path {0}: {1}")]
    CanonicalizePath(String, #[source] std::io::Error),
    #[error("failed to load module {0}: {1}")]
    LoadModule(String, #[source] AnyError),
    #[error("failed to evaluate module {0}: {1}")]
    EvaluateModule(String, #[source] AnyError),
    #[error("failed to get namespace of module {0}: {1}")]
    GetModuleNamespace(String, #[source] AnyError),
    #[error("module {0} has no default export")]
    ModuleNoDefaultExport(String),
    #[error("module {0} default export is not a function: {1}")]
    ModuleDefaultExportNotFunction(String, #[source] v8::DataError),
}

impl Runtime {
    /// Returns `Runtime` and the ids of the exported functions.
    pub async fn new<T: (FnOnce() -> Extension) + Send + 'static>(
        modules: Vec<String>,
        extension_generators: Vec<T>,
    ) -> Result<(Self, Vec<NonZeroI32>), Error> {
        let (init_sender, init_receiver) = oneshot::channel();
        let (work_sender, work_receiver) = mpsc::channel(10);
        let handle = std::thread::spawn(move || {
            let extensions = extension_generators
                .into_iter()
                .map(|generate| generate())
                .collect::<Vec<_>>();
            let mut worker = match Worker::new(modules, extensions) {
                Ok(worker) => worker,
                Err(e) => {
                    let _ = init_sender.send(Err(e));
                    return;
                }
            };
            if init_sender
                .send(Ok(worker.functions.keys().cloned().collect()))
                .is_err()
            {
                return;
            }
            worker.run(work_receiver)
        });

        let mut this = Self {
            work_sender,
            handle: Some(handle),
        };
        let functions = match init_receiver.await {
            Ok(Ok(functions)) => functions,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                this.propagate_panic();
            }
        };

        Ok((this, functions))
    }

    pub async fn call_function(
        &mut self,
        id: NonZeroI32,
        args: Vec<JsonValue>,
    ) -> Result<JsonValue, AnyError> {
        let (return_sender, return_receiver) = oneshot::channel();
        if self
            .work_sender
            .send(Work::CallFunction {
                id,
                args,
                return_sender,
            })
            .await
            .is_err()
        {
            self.propagate_panic();
        }
        let Ok(result) = return_receiver.await else {
            self.propagate_panic();
        };
        result
    }

    fn propagate_panic(&mut self) -> ! {
        self.handle
            .take()
            .expect("runtime panicked before and cannot be used again")
            .join()
            .unwrap();
        unreachable!("we should have panicked");
    }
}

struct Worker {
    tokio_runtime: tokio::runtime::Runtime,
    js_runtime: JsRuntime,
    functions: HashMap<NonZeroI32, Global<Function>>,
}

impl Worker {
    fn new(modules: Vec<String>, extensions: Vec<Extension>) -> Result<Self, Error> {
        let tokio_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::CreateJsRuntime)?;
        let mut js_runtime = js_runtime::new(extensions).map_err(Error::CreateJsRuntime)?;

        let mut functions = HashMap::with_capacity(modules.len());
        for module in modules {
            let (id, fun) = tokio_runtime.block_on(Self::load_function(&mut js_runtime, module))?;
            functions.insert(id, fun);
        }

        Ok(Self {
            tokio_runtime,
            js_runtime,
            functions,
        })
    }

    async fn call_function(
        runtime: &mut JsRuntime,
        function: NonZeroI32,
        args: Vec<JsonValue>,
        functions: &HashMap<NonZeroI32, Global<Function>>,
    ) -> Result<JsonValue, AnyError> {
        let function = functions
            .get(&function)
            .context(format!("function {} not found", function))?;
        let mut scope = runtime.handle_scope();
        let recv = undefined(&mut scope);
        let args = args
            .into_iter()
            .map(|arg| to_v8(&mut scope, arg))
            .collect::<Result<Vec<_>, _>>()?;
        let Some(promise) = Local::new(&mut scope, function).call(&mut scope, recv.into(), &args)
        else {
            // Deno doesn't expose a way to get the exception.
            bail!("uncaught javascript exception");
        };
        let promise = Global::new(&mut scope, promise);
        drop(scope);
        let result = runtime.resolve(promise);
        runtime
            .run_event_loop(deno_runtime::deno_core::PollEventLoopOptions {
                wait_for_inspector: false,
                pump_v8_message_loop: true,
            })
            .await?;
        let result = result.await?;
        let scope = &mut runtime.handle_scope();
        let result = Local::new(scope, result);
        from_v8(scope, result)
    }

    fn run(&mut self, mut work_receiver: mpsc::Receiver<Work>) {
        let runtime = &mut self.js_runtime;
        let functions = &self.functions;
        self.tokio_runtime.block_on(async {
            while let Some(work) = work_receiver.recv().await {
                match work {
                    Work::CallFunction {
                        id,
                        args,
                        return_sender,
                    } => {
                        // Ignore error if receiver is closed.
                        let _ = return_sender
                            .send(Self::call_function(runtime, id, args, functions).await);
                    }
                }
            }
        })
    }
    async fn load_function(
        runtime: &mut JsRuntime,
        module: String,
    ) -> Result<(NonZeroI32, Global<Function>), Error> {
        let path = canonicalize(&module).map_err(|e| Error::CanonicalizePath(module.clone(), e))?;
        let module_specifier =
            ModuleSpecifier::from_file_path(path).expect("we just canonicalized it");
        info!("loading module {}", module_specifier);
        let module_id = runtime
            .load_side_es_module(&module_specifier)
            .await
            .map_err(|e| Error::LoadModule(module.clone(), e))?;
        js_runtime::evaluate_module(runtime, module_id)
            .await
            .map_err(|e| Error::EvaluateModule(module.clone(), e))?;
        let namespace = runtime
            .get_module_namespace(module_id)
            .map_err(|e| Error::GetModuleNamespace(module.clone(), e))?;
        let scope = &mut runtime.handle_scope();
        let namespace = v8::Local::new(scope, namespace);
        let default_key = v8::String::new_external_onebyte_static(scope, b"default")
            .unwrap()
            .into();
        let default_export = namespace
            .get(scope, default_key)
            .ok_or_else(|| Error::ModuleNoDefaultExport(module.clone()))?;
        let function: Local<Function> = default_export
            .try_into()
            .map_err(|e| Error::ModuleDefaultExportNotFunction(module.clone(), e))?;
        let id = function.get_identity_hash();
        Ok((id, Global::new(scope, function)))
    }
}

#[derive(Debug)]
enum Work {
    CallFunction {
        id: NonZeroI32,
        args: Vec<JsonValue>,
        return_sender: oneshot::Sender<Result<JsonValue, AnyError>>,
    },
}

mod conversion;
mod js_runtime;
#[cfg(test)]
mod tests;
