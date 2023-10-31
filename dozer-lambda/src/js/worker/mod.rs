use std::{
    cell::RefCell,
    collections::HashMap,
    future::{poll_fn, Future},
    num::NonZeroI32,
    ops::ControlFlow,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};

use deno_runtime::{
    deno_core::{self, error::AnyError, extension, op2, JsRuntime, ModuleSpecifier},
    deno_napi::v8,
    permissions::PermissionsContainer,
    worker::{MainWorker, WorkerOptions},
};
use dozer_log::{
    errors::ReaderBuilderError,
    tokio::{
        runtime::Runtime,
        sync::{
            mpsc::{channel, error::TryRecvError, Receiver, Sender},
            oneshot, Mutex,
        },
        task::{JoinError, JoinHandle, LocalSet},
    },
};
use dozer_types::{
    log::{error, info},
    thiserror,
    tracing::trace,
    types::{Field, Operation},
};

use super::trigger::Trigger;

#[derive(Debug, Clone)]
pub struct Worker {
    work_sender: Sender<Work>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to initialize JavaScript lambda runtime: {0:?}")]
    InitPanic(#[source] Option<JoinError>),
    #[error("failed to canonicalize path {0}: {1}")]
    CanonicalizePath(String, #[source] std::io::Error),
    #[error("failed to execute registration script: {0}")]
    ExecuteRegistrationScript(#[from] AnyError),
}

impl Worker {
    pub async fn new(
        runtime: Arc<Runtime>,
        trigger: Arc<Mutex<Trigger>>,
        registration_scripts: Vec<String>,
    ) -> Result<Self, Error> {
        let (init_sender, init_receiver) = oneshot::channel();
        let (work_sender, work_receiver) = channel(1);
        let runtime_clone = runtime.clone();
        let handle = runtime.spawn_blocking(move || {
            let lambdas = Rc::new(RefCell::new(HashMap::new()));
            let mut main_worker = MainWorker::bootstrap_from_options(
                "https://github.com".try_into().unwrap(), // This doesn't matter. We never execute it.
                PermissionsContainer::allow_all(),
                WorkerOptions {
                    extensions: vec![dozer_lambda_extension::init_ops(trigger, lambdas.clone())],
                    ..Default::default()
                },
            );
            let local_set = LocalSet::new();
            let _ = init_sender.send(local_set.block_on(
                &runtime_clone,
                execute_registration_scripts(&mut main_worker, registration_scripts),
            ));
            local_set.block_on(
                &runtime_clone,
                worker_loop(main_worker.js_runtime, work_receiver, lambdas),
            );
        });
        match init_receiver.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return Err(e);
            }
            Err(_) => return Err(Error::InitPanic(handle.await.err())),
        }
        Ok(Self {
            work_sender,
            handle: Arc::new(Mutex::new(Some(handle))),
        })
    }

    pub async fn call_lambda(
        &self,
        func: NonZeroI32,
        operation_index: u64,
        operation: Operation,
        field_names: Vec<String>,
    ) {
        let (operation_type, new_values, old_values) = match operation {
            Operation::Insert { new } => ("insert", new.values, None),
            Operation::Update { new, old } => ("update", new.values, Some(old.values)),
            Operation::Delete { old } => ("delete", old.values, None),
        };
        if self
            .work_sender
            .send(Work::CallLambda {
                func,
                operation_index,
                operation_type,
                field_names,
                new_values,
                old_values,
            })
            .await
            .is_err()
        {
            error!("lambda runtime has panicked");
            if let Some(handle) = self.handle.lock().await.take() {
                if let Err(e) = handle.await {
                    error!("lambda runtime panicked: {}", e);
                }
            }
        }
    }
}

/// Registration script should call `register_lambda`.
async fn execute_registration_scripts(
    main_worker: &mut MainWorker,
    registration_scripts: Vec<String>,
) -> Result<(), Error> {
    for registration_script in registration_scripts {
        let path = std::fs::canonicalize(registration_script.clone())
            .map_err(|e| Error::CanonicalizePath(registration_script, e))?;
        let module_specifier =
            ModuleSpecifier::from_file_path(path).expect("we just canonicalized it");
        info!("executing module {}", module_specifier);
        main_worker.execute_side_module(&module_specifier).await?;
    }
    Ok(())
}

type LambdaHashMap = HashMap<NonZeroI32, v8::Global<v8::Function>>;

#[op2(async)]
fn register_lambda(
    #[state] trigger: &Arc<Mutex<Trigger>>,
    #[state] lambdas: &Rc<RefCell<LambdaHashMap>>,
    #[string] endpoint: String,
    lambda: &v8::Function,
    #[global] lambda_global: v8::Global<v8::Function>,
) -> impl Future<Output = Result<(), ReaderBuilderError>> {
    register_lambda_impl(
        trigger.clone(),
        lambdas.clone(),
        endpoint,
        lambda.get_identity_hash(),
        lambda_global,
    )
}

async fn register_lambda_impl(
    trigger: Arc<Mutex<Trigger>>,
    lambdas: Rc<RefCell<LambdaHashMap>>,
    endpoint: String,
    identity: NonZeroI32,
    lambda_global: v8::Global<v8::Function>,
) -> Result<(), ReaderBuilderError> {
    info!("registering lambda {} for endpoint {}", identity, endpoint);
    trigger.lock().await.add_lambda(endpoint, identity).await?;
    lambdas.borrow_mut().insert(identity, lambda_global);
    Ok(())
}

extension!(
    dozer_lambda_extension,
    ops = [register_lambda],
    options = {
        trigger: Arc<Mutex<Trigger>>,
        lambdas: Rc<RefCell<LambdaHashMap>>
    },
    state = |state, options| {
        state.put(options.trigger);
        state.put(options.lambdas);
    },
);

#[derive(Debug)]
enum Work {
    CallLambda {
        func: NonZeroI32,
        operation_index: u64,
        operation_type: &'static str,
        field_names: Vec<String>,
        new_values: Vec<Field>,
        old_values: Option<Vec<Field>>,
    },
}

async fn worker_loop(
    mut runtime: JsRuntime,
    mut work_receiver: Receiver<Work>,
    lambdas: Rc<RefCell<LambdaHashMap>>,
) {
    loop {
        match poll_fn(|cx| {
            poll_work_and_event_loop(&mut runtime, &mut work_receiver, &lambdas.borrow(), cx)
        })
        .await
        {
            ControlFlow::Continue(Ok(())) => {}
            ControlFlow::Continue(Err(e)) => {
                error!("JavaScript lambda runtime error: {}", e);
                break;
            }
            ControlFlow::Break(()) => {
                break;
            }
        }
    }
}

fn poll_work_and_event_loop(
    runtime: &mut JsRuntime,
    work_receiver: &mut Receiver<Work>,
    lambdas: &LambdaHashMap,
    cx: &mut Context,
) -> Poll<ControlFlow<(), Result<(), AnyError>>> {
    match work_receiver.try_recv() {
        Ok(work) => {
            if let Err(e) = do_work(runtime, work, lambdas) {
                return Poll::Ready(ControlFlow::Continue(Err(e)));
            }
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            return Poll::Ready(ControlFlow::Break(()));
        }
    }

    runtime
        .poll_event_loop(cx, false)
        .map(ControlFlow::Continue)
}

fn do_work(runtime: &mut JsRuntime, work: Work, lambdas: &LambdaHashMap) -> Result<(), AnyError> {
    match work {
        Work::CallLambda {
            func,
            operation_index,
            operation_type,
            field_names,
            new_values,
            old_values,
        } => {
            trace!(
                "calling lambda {} with op position {}",
                func,
                operation_index
            );
            let func = lambdas
                .get(&func)
                .unwrap_or_else(|| panic!("lambda {} not found", func));
            let mut scope = runtime.handle_scope();
            let recv = v8::undefined(&mut scope);
            let arg = conversion::operation_to_v8_value(
                &mut scope,
                operation_index as f64,
                operation_type,
                &field_names,
                new_values,
                old_values,
            )?;
            v8::Local::new(&mut scope, func).call(&mut scope, recv.into(), &[arg]);
            Ok(())
        }
    }
}

mod conversion;
