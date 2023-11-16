use std::{path::Path, time::SystemTime};

use super::fs::atomic_write_file;

/// Permissions used to save a file in the disk caches.
pub const CACHE_PERM: u32 = 0o644;

#[derive(Debug, Clone)]
pub struct RealDenoCacheEnv;

impl deno_cache_dir::DenoCacheEnv for RealDenoCacheEnv {
    fn read_file_bytes(&self, path: &Path) -> std::io::Result<Option<Vec<u8>>> {
        match std::fs::read(path) {
            Ok(s) => Ok(Some(s)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn atomic_write_file(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        atomic_write_file(path, bytes, CACHE_PERM)
    }

    fn modified(&self, path: &Path) -> std::io::Result<Option<SystemTime>> {
        match std::fs::metadata(path) {
            Ok(metadata) => Ok(Some(
                metadata.modified().unwrap_or_else(|_| SystemTime::now()),
            )),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn is_file(&self, path: &Path) -> bool {
        path.is_file()
    }

    fn time_now(&self) -> SystemTime {
        SystemTime::now()
    }
}

pub type GlobalHttpCache = deno_cache_dir::GlobalHttpCache<RealDenoCacheEnv>;
