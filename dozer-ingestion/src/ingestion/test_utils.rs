use dozer_types::models::source::Source;
use std::fs;

pub fn load_config(config_path: String) -> Result<Source, serde_yaml::Error> {
    let contents = fs::read_to_string(config_path).unwrap();

    serde_yaml::from_str::<Source>(&contents)
}
