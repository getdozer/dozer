use crate::errors::ConfigCombineError;
use crate::errors::ConfigCombineError::{
    CannotReadConfig, CannotReadFile, CannotSerializeToString, WrongPatternOfConfigFilesGlob,
};
use dozer_types::serde_yaml;
use dozer_types::serde_yaml::mapping::Entry;
use dozer_types::serde_yaml::Mapping;
use glob::glob;
use std::fs;

pub fn combine_config(config_path: &str) -> Result<String, ConfigCombineError> {
    let mut combined_yaml = serde_yaml::Value::Mapping(Mapping::new());
    let mut sqls = vec![];
    let patterns: Vec<_> = config_path.split(',').collect();
    for pattern in patterns {
        let files_glob = glob(pattern).map_err(WrongPatternOfConfigFilesGlob)?;

        for entry in files_glob {
            let path = entry.map_err(CannotReadFile)?;
            if let Some(name) = path.clone().to_str() {
                let content =
                    fs::read_to_string(path.clone()).map_err(|e| CannotReadConfig(path, e))?;

                if name.contains(".yml") || name.contains(".yaml") {
                    let yaml: serde_yaml::Value = serde_yaml::from_str(&content)
                        .map_err(|e| ConfigCombineError::ParseYaml(name.to_string(), e))?;
                    merge_yaml(yaml, &mut combined_yaml)?;
                } else if name.contains(".sql") {
                    let sql = if content.ends_with(';') {
                        content.clone()
                    } else {
                        content.clone() + ";"
                    };

                    sqls.push(sql);
                }
            }
        }
    }

    if !sqls.is_empty() {
        let joined_sql = sqls.join(" ");
        let yaml = serde_yaml::Value::Mapping(Mapping::from_iter([(
            serde_yaml::Value::String("sql".into()),
            serde_yaml::Value::String(joined_sql),
        )]));
        merge_yaml(yaml, &mut combined_yaml)?;
    }

    // `serde_yaml::from_value` will return deserialization error, not sure why.
    serde_yaml::to_string(&combined_yaml).map_err(CannotSerializeToString)
}

fn merge_yaml(
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
