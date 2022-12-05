use include_dir::{include_dir, Dir};
static TESTS_CONFIG_DIR: Dir<'_> = include_dir!("config/tests/local");

pub fn load_config(file_name: &str) -> &str {
    TESTS_CONFIG_DIR
        .get_file(file_name)
        .unwrap()
        .contents_utf8()
        .unwrap()
}
