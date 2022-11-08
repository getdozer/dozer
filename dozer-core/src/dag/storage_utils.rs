// use crate::dag::errors::ExecutionError;
// use crate::dag::node::NodeHandle;
// use crate::storage::errors::StorageError;
// use crate::storage::errors::StorageError::InternalDbError;
// use crate::storage::lmdb_sys::{EnvOptions, Environment, Transaction};
// use libc::size_t;
// use std::path::{Path, PathBuf};
//
// const DEFAULT_MAX_DBS: u32 = 256;
// const DEFAULT_MAX_READERS: u32 = 256;
// const DEFAULT_MAX_MAP_SZ: size_t = 1024 * 1024 * 1024 * 64;
// const DEFAULT_COMMIT_SZ: u16 = 10_000;
// const CHECKPOINT_DB_NAME: &str = "__CHECKPOINT_META";
//
// struct LmdbStorageMetadata {
//     env: Environment,
//     tx: Transaction,
// }
//
// struct StorageMetadata<T> {
//     tx: Box<dyn RwTransaction>,
//     db: Database,
// }
//
// fn start_env(base_path: PathBuf, name: String) -> Result<Environment, StorageError> {
//     let full_path = base_path.join(Path::new(name.as_str()));
//
//     let mut env_opt = EnvOptions::default();
//
//     env_opt.max_dbs = Some(DEFAULT_MAX_DBS);
//     env_opt.map_size = Some(DEFAULT_MAX_MAP_SZ);
//     env_opt.max_readers = Some(DEFAULT_MAX_READERS);
//     env_opt.writable_mem_map = true;
//     env_opt.no_subdir = true;
//     env_opt.no_thread_local_storage = true;
//     env_opt.no_locking = true;
//
//     Environment::new(full_path.to_str().unwrap().to_string(), env_opt).map_err(InternalDbError)
// }

// fn init_component<F>(
//     node_handle: NodeHandle,
//     base_path: PathBuf,
//     stateful: bool,
//     shared: bool,
//     init_f: F,
// ) -> Result<Option<StorageMetadata>, ExecutionError>
// where
//     F: Fn(Option<&mut dyn RwTransaction>) -> Result<(), ExecutionError>,
// {
//
//     match stateful {
//         false => {
//             let _ = init_f(None)?;
//             Ok(None)
//         }
//         true => {
//
//             let mut env = start_env(base_path, node_handle.to_string())?;
//                     let mut txn = env.tx_begin(false)?;
//                     proc.init(Some(&mut txn))?;
//                     txn.commit()?;
//                     Some(env)
//
//
//             match shared {
//                 false => {Ok(None)}
//                 true => {
//                     Ok(None)
//                 }
//             }
//         }
//     }
//

//}
