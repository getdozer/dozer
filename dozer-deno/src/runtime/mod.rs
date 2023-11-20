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
        anyhow::{anyhow, Context as _},
        error::AnyError,
        futures::future::Either,
        serde_v8::{from_v8, to_v8},
        JsRuntime, ModuleSpecifier,
    },
    deno_napi::v8::{self, undefined, Function, Global, HandleScope, Local, Promise, PromiseState},
};
use dozer_types::{
    log::{error, info},
    serde_json::Value,
    thiserror,
};
use tokio::{
    sync::{
        mpsc::{self, error::TryRecvError},
        oneshot,
    },
    task::{JoinHandle, LocalSet},
};

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
    pub async fn new(
        tokio_runtime: Arc<tokio::runtime::Runtime>,
        modules: Vec<String>,
    ) -> Result<(Self, Vec<NonZeroI32>), Error> {
        let (init_sender, init_receiver) = oneshot::channel();
        let (work_sender, work_receiver) = mpsc::channel(10);
        let handle = tokio_runtime.clone().spawn_blocking(move || {
            let mut js_runtime = match js_runtime::new() {
                Ok(js_runtime) => js_runtime,
                Err(e) => {
                    let _ = init_sender.send(Err(Error::CreateJsRuntime(e)));
                    return;
                }
            };
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
                worker_loop(js_runtime, work_receiver, functions),
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
                handle: Some(handle),
            },
            functions,
        ))
    }

    pub async fn call_function(
        &mut self,
        id: NonZeroI32,
        args: Vec<Value>,
    ) -> Result<Value, AnyError> {
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
            return self.propagate_panic().await;
        }
        let Ok(result) = return_receiver.await else {
            return self.propagate_panic().await;
        };
        result
    }

    // Return type is actually `!`
    async fn propagate_panic(&mut self) -> Result<Value, AnyError> {
        self.handle
            .take()
            .expect("runtime panicked before and cannot be used again")
            .await
            .unwrap();
        unreachable!("we should have panicked");
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
    CallFunction {
        id: NonZeroI32,
        args: Vec<Value>,
        return_sender: oneshot::Sender<Result<Value, AnyError>>,
    },
}

#[derive(Debug)]
struct FunctionReturnPromise {
    promise: Global<Promise>,
    return_sender: oneshot::Sender<Result<Value, AnyError>>,
}

impl FunctionReturnPromise {
    fn return_if_resolved(self, scope: &mut HandleScope) -> Option<Self> {
        let promise = Local::new(scope, self.promise);
        match promise.state() {
            // Ignore error if receiver is closed.
            PromiseState::Fulfilled => {
                let result = promise.result(scope);
                let _ = self
                    .return_sender
                    .send(from_v8(scope, result).map_err(Into::into));
                None
            }
            PromiseState::Rejected => {
                let _ = self.return_sender.send(Err(anyhow!("promise rejected")));
                None
            }
            PromiseState::Pending => {
                let promise = Global::new(scope, promise);
                Some(Self {
                    promise,
                    return_sender: self.return_sender,
                })
            }
        }
    }
}

async fn worker_loop(
    mut runtime: JsRuntime,
    mut work_receiver: mpsc::Receiver<Work>,
    functions: HashMap<NonZeroI32, Global<Function>>,
) {
    let mut pending = vec![];
    loop {
        match poll_fn(|cx| {
            poll_work_and_event_loop(
                &mut runtime,
                &mut work_receiver,
                &mut pending,
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
    pending: &mut Vec<FunctionReturnPromise>,
    functions: &HashMap<NonZeroI32, Global<Function>>,
    cx: &mut Context,
) -> Poll<ControlFlow<(), Result<(), AnyError>>> {
    match work_receiver.try_recv() {
        Ok(work) => {
            pending.extend(do_work(runtime, work, functions));
        }
        Err(TryRecvError::Empty) => (),
        Err(TryRecvError::Disconnected) => return Poll::Ready(ControlFlow::Break(())),
    }

    let result = runtime
        .poll_event_loop(cx, false)
        .map(ControlFlow::Continue);

    let scope = &mut runtime.handle_scope();
    *pending = std::mem::take(pending)
        .into_iter()
        .flat_map(|promise| promise.return_if_resolved(scope))
        .collect();

    result
}

fn do_work(
    runtime: &mut JsRuntime,
    work: Work,
    functions: &HashMap<NonZeroI32, Global<Function>>,
) -> Option<FunctionReturnPromise> {
    match work {
        Work::CallFunction {
            id,
            args,
            return_sender,
        } => {
            // Ignore error if receiver is closed.
            match call_function(runtime, id, args, functions) {
                Ok(Either::Left(result)) => {
                    let _ = return_sender.send(Ok(result));
                    None
                }
                Err(e) => {
                    let _ = return_sender.send(Err(e));
                    None
                }
                Ok(Either::Right(promise)) => Some(FunctionReturnPromise {
                    promise,
                    return_sender,
                }),
            }
        }
    }
}

fn call_function(
    runtime: &mut JsRuntime,
    function: NonZeroI32,
    args: Vec<Value>,
    functions: &HashMap<NonZeroI32, Global<Function>>,
) -> Result<Either<Value, Global<Promise>>, AnyError> {
    let function = functions
        .get(&function)
        .context(format!("function {} not found", function))?;
    let scope = &mut runtime.handle_scope();
    let recv = undefined(scope);
    let args = args
        .into_iter()
        .map(|arg| to_v8(scope, arg))
        .collect::<Result<Vec<_>, _>>()?;
    let Some(result) = Local::new(scope, function).call(scope, recv.into(), &args) else {
        return Ok(Either::Left(Value::Null));
    };
    if let Ok(promise) = TryInto::<Local<'_, Promise>>::try_into(result) {
        Ok(Either::Right(Global::new(scope, promise)))
    } else {
        from_v8(scope, result).map_err(Into::into).map(Either::Left)
    }
}

mod js_runtime;
