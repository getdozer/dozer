//! `JsRuntime` is `!Send + !Sync`, make it difficult to use.
//! Here we implement a `Runtime` struct that runs `JsRuntime` in a dedicated thread.
//! By sending work to the worker thread, `Runtime` is `Send + Sync`.

use std::{
    collections::HashMap,
    fs::canonicalize,
    future::poll_fn,
    num::NonZeroI32,
    ops::ControlFlow,
    sync::Arc,
    task::{Context, Poll},
};

use deno_runtime::{
    deno_core::{
        anyhow::Context as _,
        error::AnyError,
        serde_v8::{from_v8, to_v8},
        JsRuntime, ModuleSpecifier,
    },
    deno_napi::v8::{self, undefined, Function, Global, Local},
};
use dozer_types::{
    log::{error, info},
    serde_json::Value,
    thiserror,
};
use tokio::{
    sync::{
        mpsc::{
            self,
            error::{TryRecvError, TrySendError},
        },
        oneshot,
    },
    task::{JoinHandle, LocalSet},
};

#[derive(Debug)]
pub struct Runtime {
    work_sender: mpsc::Sender<Work>,
    return_receiver: mpsc::Receiver<Return>,
    handle: JoinHandle<()>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
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
    pub async fn new(
        tokio_runtime: Arc<tokio::runtime::Runtime>,
        modules: Vec<String>,
    ) -> Result<(Self, Vec<NonZeroI32>), Error> {
        let (init_sender, init_receiver) = oneshot::channel();
        let (work_sender, work_receiver) = mpsc::channel(1);
        let (return_sender, return_receiver) = mpsc::channel(1);
        let handle = tokio_runtime.clone().spawn_blocking(move || {
            let mut js_runtime = js_runtime::new();
            let local_set = LocalSet::new();
            let functions = match local_set
                .block_on(&tokio_runtime, load_functions(&mut js_runtime, modules))
            {
                Ok(functions) => {
                    if init_sender
                        .send(Ok(functions.iter().map(|(id, _)| *id).collect::<Vec<_>>()))
                        .is_err()
                    {
                        return;
                    }
                    functions
                }
                Err(e) => {
                    let _ = init_sender.send(Err(e));
                    return;
                }
            };
            let functions = functions.into_iter().collect();
            local_set.block_on(
                &tokio_runtime,
                worker_loop(js_runtime, work_receiver, return_sender, functions),
            );
        });

        let functions = match init_receiver.await {
            Ok(Ok(functions)) => functions,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                // Propagate the panic.
                handle.await.unwrap();
                unreachable!("we should have panicked");
            }
        };

        Ok((
            Self {
                work_sender,
                return_receiver,
                handle,
            },
            functions,
        ))
    }

    pub async fn call_function(mut self, id: NonZeroI32, args: Vec<Value>) -> (Self, Value) {
        if self
            .work_sender
            .send(Work::CallFunction { id, args })
            .await
            .is_err()
        {
            // Propagate the panic.
            self.handle.await.unwrap();
            unreachable!("we should have panicked");
        }
        let Some(result) = self.return_receiver.recv().await else {
            // Propagate the panic.
            self.handle.await.unwrap();
            unreachable!("we should have panicked");
        };
        let Return::CallFunction(result) = result;
        (self, result)
    }
}

async fn load_functions(
    runtime: &mut JsRuntime,
    modules: Vec<String>,
) -> Result<Vec<(NonZeroI32, Global<Function>)>, Error> {
    let mut result = vec![];
    for module in modules {
        let path = canonicalize(&module).map_err(|e| Error::CanonicalizePath(module.clone(), e))?;
        let module_specifier =
            ModuleSpecifier::from_file_path(path).expect("we just canonicalized it");
        info!("loading module {}", module_specifier);
        let module_id = runtime
            .load_side_module(&module_specifier, None)
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
        result.push((id, Global::new(scope, function)));
    }
    Ok(result)
}

#[derive(Debug)]
enum Work {
    CallFunction { id: NonZeroI32, args: Vec<Value> },
}

#[derive(Debug)]
enum Return {
    CallFunction(Value),
}

async fn worker_loop(
    mut runtime: JsRuntime,
    mut work_receiver: mpsc::Receiver<Work>,
    return_sender: mpsc::Sender<Return>,
    functions: HashMap<NonZeroI32, Global<Function>>,
) {
    loop {
        match poll_fn(|cx| {
            poll_work_and_event_loop(
                &mut runtime,
                &mut work_receiver,
                &return_sender,
                &functions,
                cx,
            )
        })
        .await
        {
            ControlFlow::Continue(Ok(())) => {}
            ControlFlow::Continue(Err(e)) => {
                error!("JavaScript runtime error: {}", e);
            }
            ControlFlow::Break(()) => {
                break;
            }
        }
    }
}

fn poll_work_and_event_loop(
    runtime: &mut JsRuntime,
    work_receiver: &mut mpsc::Receiver<Work>,
    return_sender: &mpsc::Sender<Return>,
    functions: &HashMap<NonZeroI32, Global<Function>>,
    cx: &mut Context,
) -> Poll<ControlFlow<(), Result<(), AnyError>>> {
    match work_receiver.try_recv() {
        Ok(work) => {
            return match do_work(runtime, work, functions) {
                Ok(value) => match return_sender.try_send(Return::CallFunction(value)) {
                    Ok(()) => Poll::Ready(ControlFlow::Continue(Ok(()))),
                    Err(TrySendError::Full(_)) => unreachable!("work can only be sent serially"),
                    Err(TrySendError::Closed(_)) => Poll::Ready(ControlFlow::Break(())),
                },
                Err(e) => Poll::Ready(ControlFlow::Continue(Err(e))),
            }
        }
        Err(TryRecvError::Empty) => (),
        Err(TryRecvError::Disconnected) => return Poll::Ready(ControlFlow::Break(())),
    }

    runtime
        .poll_event_loop(cx, false)
        .map(ControlFlow::Continue)
}

fn do_work(
    runtime: &mut JsRuntime,
    work: Work,
    functions: &HashMap<NonZeroI32, Global<Function>>,
) -> Result<Value, AnyError> {
    match work {
        Work::CallFunction { id, args } => {
            let function = functions
                .get(&id)
                .context(format!("function {} not found", id))?;
            let scope = &mut runtime.handle_scope();
            let recv = undefined(scope);
            let args = args
                .into_iter()
                .map(|arg| to_v8(scope, arg))
                .collect::<Result<Vec<_>, _>>()?;
            let result = Local::new(scope, function).call(scope, recv.into(), &args);
            result
                .map(|value| from_v8(scope, value).map_err(Into::into))
                .unwrap_or(Ok(Value::Null))
        }
    }
}

mod js_runtime;
