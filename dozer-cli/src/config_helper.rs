use crate::errors::ConfigCombineError;
use crate::errors::ConfigCombineError::{
    CannotReadConfig, CannotReadFile, CannotSerializeToString, WrongPatternOfConfigFilesGlob,
};
use dozer_types::log::warn;
use dozer_types::serde_yaml;
use dozer_types::serde_yaml::mapping::Entry;
use dozer_types::serde_yaml::Mapping;
use glob::glob;
use std::fs;

pub fn combine_config(config_paths: Vec<String>) -> Result<Option<String>, ConfigCombineError> {
    let mut combined_yaml = serde_yaml::Value::Mapping(Mapping::new());

    let mut config_found = false;
    for pattern in config_paths {
        let files_glob = glob(&pattern).map_err(WrongPatternOfConfigFilesGlob)?;

        for entry in files_glob {
            let path = entry.map_err(CannotReadFile)?;
            match path.clone().to_str() {
                None => {
                    warn!("[Config] Path {:?} is not valid", path)
                }
                Some(name) => {
                    let content =
                        fs::read_to_string(path.clone()).map_err(|e| CannotReadConfig(path, e))?;

                    if name.contains(".yml") || name.contains(".yaml") {
                        config_found = true;
                    }

                    add_file_content_to_config(&mut combined_yaml, name, content)?;
                }
            }
        }
    }

    if config_found {
        // `serde_yaml::from_value` will return deserialization error, not sure why.
        serde_yaml::to_string(&combined_yaml)
            .map_err(CannotSerializeToString)
            .map(Some)
    } else {
        Ok(None)
    }
}

pub fn add_file_content_to_config(
    combined_yaml: &mut serde_yaml::Value,
    name: &str,
    content: String,
) -> Result<(), ConfigCombineError> {
    if name.contains(".yml") || name.contains(".yaml") {
        let yaml: serde_yaml::Value = serde_yaml::from_str(&content)
            .map_err(|e| ConfigCombineError::ParseYaml(name.to_string(), e))?;
        merge_yaml(yaml, combined_yaml, false)?;
    } else if name.contains(".sql") {
        let yaml = serde_yaml::Value::Mapping(Mapping::from_iter([(
            serde_yaml::Value::String("sql".into()),
            serde_yaml::Value::String(";".to_string() + content.as_str()),
        )]));
        merge_yaml(yaml, combined_yaml, true)?;
    } else {
        warn!("Config file \"{name}\" extension not supported");
    }

    Ok(())
}

pub fn merge_yaml(
    from: serde_yaml::Value,
    to: &mut serde_yaml::Value,
    join_strings: bool,
) -> Result<(), ConfigCombineError> {
    match (from, to) {
        (serde_yaml::Value::Mapping(from), serde_yaml::Value::Mapping(to)) => {
            for (key, value) in from {
                match to.entry(key) {
                    Entry::Occupied(mut entry) => {
                        merge_yaml(value, entry.get_mut(), join_strings)?;
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(value);
                    }
                }
            }
            Ok(())
        }
        (serde_yaml::Value::Sequence(from), serde_yaml::Value::Sequence(to)) => {
            for value in from {
                to.push(value);
            }
            Ok(())
        }
        (serde_yaml::Value::Tagged(from), serde_yaml::Value::Tagged(to)) => {
            if from.tag != to.tag {
                return Err(ConfigCombineError::CannotMerge {
                    from: serde_yaml::Value::Tagged(from),
                    to: serde_yaml::Value::Tagged(to.clone()),
                });
            }
            merge_yaml(from.value, &mut to.value, join_strings)
        }
        (serde_yaml::Value::String(from), serde_yaml::Value::String(to)) if join_strings => {
            to.push_str(&from);
            Ok(())
        }
        (from, to) => Err(ConfigCombineError::CannotMerge {
            from,
            to: to.clone(),
        }),
    }
}
