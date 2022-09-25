use lmdb::{DatabaseFlags, Environment, Transaction, WriteFlags};
use std::fs;
use std::path::Path;
fn main() {
    fs::create_dir_all("target/cache.mdb").unwrap();

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

    let mut txn = env.begin_rw_txn().unwrap();

    txn.put::<String, String>(
        db,
        &"hello".to_string(),
        &"yes".to_string(),
        WriteFlags::default(),
    )
    .unwrap();

    let val = txn.get::<String>(db, &"hello".to_string()).unwrap();
    let val = std::str::from_utf8(val).unwrap();

    println!("{}", val);
}
