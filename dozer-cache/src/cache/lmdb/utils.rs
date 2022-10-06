use lmdb::{Database, DatabaseFlags, Environment};
use std::fs;
use std::path::Path;
use tempdir::TempDir;

pub fn init_db(temp: bool) -> anyhow::Result<(Environment, Database)> {
    let env = match temp {
        true => Environment::new().open(TempDir::new("schema-registry").unwrap().path())?,
        false => {
            fs::create_dir_all("target/cache.mdb")?;

            Environment::new().open(
                Path::new("target/cache.mdb")
                    .canonicalize()
                    .unwrap()
                    .as_path(),
            )?
        }
    };

    let mut flags = DatabaseFlags::default();
    flags.set(DatabaseFlags::DUP_SORT, true);

    let db = env.create_db(None, flags)?;
    // (&env, &db)

    let size = 1024 * 1024 * 1024 * 5;
    env.set_map_size(size)?;

    Ok((env, db))
}
