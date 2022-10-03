use lmdb::{Database, DatabaseFlags, Environment};
use std::fs;
use std::path::Path;
use tempdir::TempDir;

pub fn init_db(temp: bool) -> (Environment, Database) {
    let env = match temp {
        true => Environment::new()
            .open(TempDir::new("schema-registry").unwrap().path())
            .unwrap(),
        false => {
            match fs::create_dir_all("target/cache.mdb") {
                Ok(_) => {}
                Err(_) => println!("path already exists"),
            };

            Environment::new()
                .open(
                    Path::new("target/cache.mdb")
                        .canonicalize()
                        .unwrap()
                        .as_path(),
                )
                .unwrap()
        }
    };

    let mut flags = DatabaseFlags::default();
    flags.set(DatabaseFlags::DUP_SORT, true);

    let db = env.create_db(None, flags).unwrap();
    // (&env, &db)
    (env, db)
}
