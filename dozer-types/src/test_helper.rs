use std::fs;
use tempdir::TempDir;
#[macro_export]
macro_rules! chk {
    ($stmt:expr) => {
        $stmt.unwrap_or_else(|e| panic!("{}", e.to_string()))
    };
}

pub fn get_temp_dir() -> String {
    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    tmp_dir
        .path()
        .to_str()
        .unwrap_or_else(|| panic!("Unable to create temp dir"))
        .to_string()
}

pub fn init_logger() {
    chk!(log4rs::init_file("../log4rs.yaml", Default::default()));
}
