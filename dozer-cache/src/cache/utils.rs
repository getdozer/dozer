use lmdb::{Database, DatabaseFlags, Environment, Transaction, WriteFlags};
use std::fs;
use std::path::Path;

pub fn init_db<'a>() -> (Environment, Database) {
    match fs::create_dir_all("target/cache.mdb") {
        Ok(_) => {}
        Err(_) => println!("path already exists"),
    };

    let env = Environment::new()
        .open(
            Path::new("target/cache.mdb")
                .canonicalize()
                .unwrap()
                .as_path(),
        )
        .unwrap();

    let mut flags = DatabaseFlags::default();
    flags.set(DatabaseFlags::DUP_SORT, true);

    let db = env.create_db(None, flags).unwrap();
    // (&env, &db)
    (env, db)
}
