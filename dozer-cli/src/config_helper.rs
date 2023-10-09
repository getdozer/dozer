use crate::errors::ConfigCombineError;
use crate::errors::ConfigCombineError::{
    CannotReadConfig, CannotReadFile, CannotSerializeToString, SqlIsNotStringType,
    WrongPatternOfConfigFilesGlob,
};
use dozer_types::log::warn;
use dozer_types::serde_yaml;
use dozer_types::serde_yaml::mapping::Entry;
use dozer_types::serde_yaml::{Mapping, Value};
use glob::glob;

use std::fs::File as FsFile;
use std::io::{BufReader, Read};

pub fn combine_config(
    config_paths: Vec<String>,
    stdin_yaml: Option<String>,
) -> Result<Option<String>, ConfigCombineError> {
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
                    let f = FsFile::open(path.clone())
                        .map_err(|e| CannotReadConfig(path.clone(), e))?;
                    let mut reader = BufReader::new(f);
                    let mut buffer = Vec::new();

                    reader
                        .read_to_end(&mut buffer)
                        .map_err(|e| CannotReadConfig(path, e))?;
                    if name.contains(".yml") || name.contains(".yaml") {
                        config_found = true;
                    }

                    add_file_content_to_config(&mut combined_yaml, name, buffer)?;
                }
            }
        }
    }
    let stdin_name = "Stdin";
    // Merge stdin_yaml content into combined_yaml if provided
    if let Some(stdin_content) = stdin_yaml {
        let stdin_yaml: serde_yaml::Value = serde_yaml::from_str(&stdin_content)
            .map_err(|e| ConfigCombineError::ParseYaml(stdin_name.to_string(), e))?; //deserialise yaml content from stdin
        merge_yaml(stdin_yaml, &mut combined_yaml)?; //merge with yaml from config-paths
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
    content: Vec<u8>,
) -> Result<(), ConfigCombineError> {
    if name.contains(".yml") || name.contains(".yaml") {
        let c = String::from_utf8(content)?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&c)
            .map_err(|e| ConfigCombineError::ParseYaml(name.to_string(), e))?;
        merge_yaml(yaml, combined_yaml)?;
    } else if name.contains(".sql") {
        let mapping = combined_yaml.as_mapping_mut().expect("Should be mapping");
        let sql = mapping.get_mut(serde_yaml::Value::String("sql".into()));

        let c = String::from_utf8(content)?;

        match sql {
            None => {
                mapping.insert(
                    serde_yaml::Value::String("sql".into()),
                    serde_yaml::Value::String(c),
                );
            }
            Some(s) => {
                let query = s.as_str();
                *s = match query {
                    None => {
                        return Err(SqlIsNotStringType);
                    }
                    Some(current_query) => {
                        Value::String(format!("{};{}", current_query, c.as_str()))
                    }
                }
            }
        }
    } else {
        warn!("Config file \"{name}\" extension not supported");
    }

    Ok(())
}

pub fn merge_yaml(
    from: serde_yaml::Value,
    to: &mut serde_yaml::Value,
) -> Result<(), ConfigCombineError> {
    match (from, to) {
        (serde_yaml::Value::Mapping(from), serde_yaml::Value::Mapping(to)) => {
            for (key, value) in from {
                match to.entry(key) {
                    Entry::Occupied(mut entry) => {
                        merge_yaml(value, entry.get_mut())?;
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
            merge_yaml(from.value, &mut to.value)
        }
        (from, to) => Err(ConfigCombineError::CannotMerge {
            from,
            to: to.clone(),
        }),
    }
}
