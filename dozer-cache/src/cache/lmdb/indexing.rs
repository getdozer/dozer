use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Barrier,
};

use dozer_storage::LmdbEnvironment;
use dozer_types::{
    log::{debug, error},
    parking_lot::Mutex,
};

use crate::errors::CacheError;

use super::cache::{LmdbRoCache, MainEnvironment, RoMainEnvironment, RwSecondaryEnvironment};

#[derive(Debug)]
pub struct IndexingThreadPool {
    num_threads: usize,
    termination_barrier: Arc<Barrier>,
    caches: Vec<Cache>,
    running: Running,
}

impl IndexingThreadPool {
    pub fn new(num_threads: usize) -> Self {
        let termination_barrier = Arc::new(Barrier::new(num_threads + 1));
        Self {
            num_threads,
            caches: Vec::new(),
            running: create_running(num_threads, termination_barrier.clone()),
            termination_barrier,
        }
    }

    pub fn add_cache(
        &mut self,
        main_env: RoMainEnvironment,
        secondary_envs: Vec<RwSecondaryEnvironment>,
    ) {
        let secondary_envs = secondary_envs
            .into_iter()
            .map(|env| Arc::new(Mutex::new(env)))
            .collect();
        let cache = Cache {
            main_env,
            secondary_envs,
        };
        add_indexing_tasks(&mut self.running, &cache);
        self.caches.push(cache);
    }

    pub fn find_cache(&self, name: &str) -> Option<LmdbRoCache> {
        for cache in self.caches.iter() {
            if cache.main_env.name() == name {
                let secondary_envs = cache
                    .secondary_envs
                    .iter()
                    .map(|env| env.lock().share())
                    .collect();
                return Some(LmdbRoCache {
                    main_env: cache.main_env.clone(),
                    secondary_envs,
                });
            }
        }
        None
    }

    pub fn wait_until_catchup(&mut self) {
        let running = std::mem::replace(
            &mut self.running,
            create_running(self.num_threads, self.termination_barrier.clone()),
        );
        drop(running);
        self.termination_barrier.wait();

        for cache in self.caches.iter() {
            add_indexing_tasks(&mut self.running, cache);
        }
    }
}

fn create_running(num_threads: usize, termination_barrier: Arc<Barrier>) -> Running {
    Running::new(num_threads, move |_| {
        termination_barrier.wait();
    })
}

fn add_indexing_tasks(running: &mut Running, cache: &Cache) {
    for secondary_env in &cache.secondary_envs {
        running.add_indexing_task(cache.main_env.clone(), secondary_env.clone());
    }
}

#[derive(Debug, Clone)]
struct Cache {
    main_env: RoMainEnvironment,
    secondary_envs: Vec<Arc<Mutex<RwSecondaryEnvironment>>>,
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
        main_env: RoMainEnvironment,
        secondary_env: Arc<Mutex<RwSecondaryEnvironment>>,
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
    main_env: RoMainEnvironment,
    secondary_env: Arc<Mutex<RwSecondaryEnvironment>>,
    running: Arc<AtomicBool>,
) {
    loop {
        let mut secondary_env = secondary_env.lock();

        // Run `index` for at least once before quitting.
        if let Err(e) = index(&main_env, &mut secondary_env) {
            debug!("Error while indexing {}: {e}", main_env.name());
            if e.is_map_full() {
                error!(
                    "Cache {} has reached its maximum size. Try to increase `cache_max_map_size` in the config.",
                    main_env.name()
                );
            }
        }

        if !running.load(Ordering::SeqCst) {
            break;
        }

        rayon::yield_local();
    }
}

fn index(
    main_env: &RoMainEnvironment,
    secondary_env: &mut RwSecondaryEnvironment,
) -> Result<(), CacheError> {
    let txn = main_env.begin_txn()?;

    let span = dozer_types::tracing::span!(dozer_types::tracing::Level::TRACE, "build_indexes",);
    let _enter = span.enter();

    secondary_env.index(&txn, main_env.operation_log())?;
    secondary_env.commit()?;
    Ok(())
}
