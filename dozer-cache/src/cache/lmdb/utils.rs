use std::fs;
use std::path::Path;

use lmdb::{Database, DatabaseFlags, Environment};
use tempdir::TempDir;

pub fn init_env(temp: bool) -> anyhow::Result<Environment> {
    let map_size = 1024 * 1024 * 1024 * 5;
    let mut env = Environment::new();

    let env = env
        .set_max_readers(10)
        .set_map_size(map_size)
        .set_max_dbs(10)
        .set_map_size(map_size);

    let env = match temp {
        true => env.open(TempDir::new("cache")?.path())?,
        false => {
            fs::create_dir_all("target/cache.mdb")?;
            env.open(Path::new("target/cache.mdb"))?
        }
    };

    Ok(env)
}

pub fn init_db(env: &Environment, name: Option<&str>) -> anyhow::Result<Database> {
    let mut flags = DatabaseFlags::default();
    flags.set(DatabaseFlags::DUP_SORT, true);

    let db = env.create_db(name, flags)?;

    Ok(db)
}
