use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};

use dozer_storage::BeginTransaction;
use dozer_types::log::error;

use crate::errors::CacheError;

use super::cache::{MainEnvironment, RoMainEnvironment, RwSecondaryEnvironment};

#[derive(Debug)]
pub struct IndexingThreadPool {
    num_threads: usize,
    termination_barrier: Arc<Barrier>,
    tasks: Vec<Task>,
    running: Running,
}

impl IndexingThreadPool {
    pub fn new(num_threads: usize) -> Self {
        let termination_barrier = Arc::new(Barrier::new(num_threads + 1));
        Self {
            num_threads,
            tasks: Vec::new(),
            running: create_running(num_threads, termination_barrier.clone()),
            termination_barrier,
        }
    }

    pub fn add_indexing_task(
        &mut self,
        main_env: Arc<RoMainEnvironment>,
        secondary_env: Arc<RwSecondaryEnvironment>,
    ) {
        self.tasks
            .push(Task::new(main_env.clone(), secondary_env.clone()));
        self.running.add_indexing_task(main_env, secondary_env);
    }

    pub fn wait_until_catchup(&mut self) {
        let running = std::mem::replace(
            &mut self.running,
            create_running(self.num_threads, self.termination_barrier.clone()),
        );
        drop(running);
        self.termination_barrier.wait();

        for task in self.tasks.iter() {
            self.running
                .add_indexing_task(task.main_env.clone(), task.secondary_env.clone());
        }
    }
}

fn create_running(num_threads: usize, termination_barrier: Arc<Barrier>) -> Running {
    Running::new(num_threads, move |_| {
        termination_barrier.wait();
    })
}

#[derive(Debug, Clone)]
struct Task {
    main_env: Arc<RoMainEnvironment>,
    secondary_env: Arc<RwSecondaryEnvironment>,
}

impl Task {
    fn new(main_env: Arc<RoMainEnvironment>, secondary_env: Arc<RwSecondaryEnvironment>) -> Self {
        Self {
            main_env,
            secondary_env,
        }
    }
}

#[derive(Debug)]
struct Running {
    inner: rayon::ThreadPool,
    running: Arc<AtomicBool>,
}

impl Running {
    fn new(num_threads: usize, exit_handler: impl Fn(usize) + Send + Sync + 'static) -> Self {
        let inner = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|index| format!("indexing-thread-{}", index))
            .exit_handler(exit_handler)
            .build()
            .unwrap();

        Self {
            inner,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    fn add_indexing_task(
        &self,
        main_env: Arc<RoMainEnvironment>,
        secondary_env: Arc<RwSecondaryEnvironment>,
    ) {
        let running = self.running.clone();
        self.inner
            .spawn(move || index_and_log_error(main_env, secondary_env, running));
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
    }
}

fn index_and_log_error(
    main_env: Arc<RoMainEnvironment>,
    secondary_env: Arc<RwSecondaryEnvironment>,
    running: Arc<AtomicBool>,
) {
    loop {
        // Run `index` for at least once before quitting.
        if let Err(e) = index(&main_env, &secondary_env) {
            error!("Error while indexing {}: {e}", main_env.name());
            std::process::abort();
        }

        if !running.load(Ordering::SeqCst) {
            break;
        }

        rayon::yield_local();
    }
}

fn index(
    main_env: &Arc<RoMainEnvironment>,
    secondary_env: &RwSecondaryEnvironment,
) -> Result<(), CacheError> {
    let txn = main_env.begin_txn()?;

    let span = dozer_types::tracing::span!(dozer_types::tracing::Level::TRACE, "build_indexes",);
    let _enter = span.enter();

    secondary_env.index(&txn, main_env.operation_log())?;
    secondary_env.commit()?;
    Ok(())
}
