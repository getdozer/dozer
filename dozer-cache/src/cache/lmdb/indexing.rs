use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};

use dozer_storage::LmdbEnvironment;
use dozer_types::{
    labels::Labels,
    log::{debug, error},
    parking_lot::Mutex,
};
use metrics::describe_counter;

use crate::{cache::lmdb::cache::SecondaryEnvironment, errors::CacheError};

use super::cache::{LmdbRoCache, MainEnvironment, RoMainEnvironment, RwSecondaryEnvironment};

const BUILD_INDEX_COUNTER_NAME: &str = "build_index";

#[derive(Debug)]
pub struct IndexingThreadPool {
    caches: Vec<Cache>,
    task_completion_sender: Sender<(usize, usize)>,
    task_completion_receiver: Receiver<(usize, usize)>,
    pool: rayon::ThreadPool,
}

impl IndexingThreadPool {
    pub fn new(num_threads: usize) -> Self {
        describe_counter!(
            BUILD_INDEX_COUNTER_NAME,
            "Number of operations built into indexes"
        );

        let (sender, receiver) = std::sync::mpsc::channel();
        Self {
            caches: Vec::new(),
            task_completion_sender: sender,
            task_completion_receiver: receiver,
            pool: create_thread_pool(num_threads),
        }
    }

    pub fn add_cache(
        &mut self,
        main_env: RoMainEnvironment,
        secondary_envs: Vec<RwSecondaryEnvironment>,
    ) {
        let num_secondary_envs = secondary_envs.len();
        let secondary_envs = secondary_envs
            .into_iter()
            .map(|env| (Arc::new(Mutex::new(env)), false))
            .collect();
        let cache = Cache {
            main_env,
            secondary_envs,
        };
        self.caches.push(cache);
        let index = self.caches.len() - 1;
        for secondary_index in 0..num_secondary_envs {
            self.spawn_task_if_not_running(index, secondary_index);
        }
    }

    pub fn find_cache(&self, labels: &Labels) -> Option<LmdbRoCache> {
        for cache in self.caches.iter() {
            if cache.main_env.labels() == labels {
                let secondary_envs = cache
                    .secondary_envs
                    .iter()
                    .map(|(env, _)| env.lock().share())
                    .collect();
                return Some(LmdbRoCache {
                    main_env: cache.main_env.clone(),
                    secondary_envs,
                });
            }
        }
        None
    }

    pub fn wake(&mut self, labels: &Labels) {
        self.refresh_task_state();
        for index in 0..self.caches.len() {
            let cache = &self.caches[index];
            if cache.main_env.labels() == labels {
                for secondary_index in 0..cache.secondary_envs.len() {
                    self.spawn_task_if_not_running(index, secondary_index);
                }
            }
        }
    }

    pub fn wait_until_catchup(&mut self) {
        while self
            .caches
            .iter()
            .any(|cache| cache.secondary_envs.iter().any(|(_, running)| *running))
        {
            let (index, secondary_index) = self
                .task_completion_receiver
                .recv()
                .expect("At least one sender is alive");
            self.mark_not_running(index, secondary_index);
        }
    }

    fn refresh_task_state(&mut self) {
        while let Ok((index, secondary_index)) = self.task_completion_receiver.try_recv() {
            self.mark_not_running(index, secondary_index);
        }
    }

    fn mark_not_running(&mut self, index: usize, secondary_index: usize) {
        let running = &mut self.caches[index].secondary_envs[secondary_index].1;
        debug_assert!(*running);
        *running = false;
    }

    fn spawn_task_if_not_running(&mut self, index: usize, secondary_index: usize) {
        let cache = &mut self.caches[index];
        let (secondary_env, running) = &mut cache.secondary_envs[secondary_index];
        if !*running {
            let main_env = cache.main_env.clone();
            let secondary_env = secondary_env.clone();
            let sender = self.task_completion_sender.clone();
            self.pool.spawn(move || {
                index_and_log_error(index, secondary_index, main_env, secondary_env, sender);
            });
            *running = true;
        }
    }
}

fn create_thread_pool(num_threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|index| format!("indexing-thread-{}", index))
        .build()
        .unwrap()
}

#[derive(Debug, Clone)]
struct Cache {
    main_env: RoMainEnvironment,
    secondary_envs: Vec<(Arc<Mutex<RwSecondaryEnvironment>>, bool)>,
}

fn index_and_log_error(
    index: usize,
    secondary_index: usize,
    main_env: RoMainEnvironment,
    secondary_env: Arc<Mutex<RwSecondaryEnvironment>>,
    task_completion_sender: Sender<(usize, usize)>,
) {
    let mut labels = main_env.labels().clone();
    labels.push("secondary_index", secondary_index.to_string());

    // Loop until map full or up to date.
    loop {
        let mut secondary_env = secondary_env.lock();

        match run_indexing(&main_env, &mut secondary_env, &labels) {
            Ok(true) => {
                break;
            }
            Ok(false) => {
                debug!(
                    "Some operation can't be read from {}: {:?}",
                    main_env.labels(),
                    secondary_env.index_definition()
                );
                rayon::yield_local();
                continue;
            }
            Err(e) => {
                debug!("Error while indexing {}: {e}", main_env.labels());
                if e.is_map_full() {
                    error!(
                        "Cache {} has reached its maximum size. Try to increase `cache_max_map_size` in the config.",
                        main_env.labels()
                    );
                    break;
                }
            }
        }
    }
    if task_completion_sender
        .send((index, secondary_index))
        .is_err()
    {
        debug!("`IndexingThreadPool` dropped while indexing task is running");
    }
}

fn run_indexing(
    main_env: &RoMainEnvironment,
    secondary_env: &mut RwSecondaryEnvironment,
    labels: &Labels,
) -> Result<bool, CacheError> {
    let txn = main_env.begin_txn()?;

    let span = dozer_types::tracing::span!(dozer_types::tracing::Level::TRACE, "build_indexes",);
    let _enter = span.enter();

    let result = secondary_env.index(
        &txn,
        main_env.operation_log(),
        BUILD_INDEX_COUNTER_NAME,
        labels,
    )?;
    secondary_env.commit()?;
    Ok(result)
}
