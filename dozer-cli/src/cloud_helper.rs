use crate::errors::ConfigCombineError::{
    CannotReadConfig, CannotReadFile, WrongPatternOfConfigFilesGlob,
};

use dozer_types::grpc_types::cloud::File;
use glob::glob;
use std::fs;

pub fn list_files(config_paths: Vec<String>) -> Result<Vec<File>, crate::errors::CloudError> {
    let mut files = vec![];
    for pattern in config_paths {
        let files_glob = glob(&pattern).map_err(WrongPatternOfConfigFilesGlob)?;

        for entry in files_glob {
            let path = entry.map_err(CannotReadFile)?;
            files.push(File {
                name: path.clone().to_str().unwrap().to_string(),
                content: fs::read_to_string(path.clone()).map_err(|e| CannotReadConfig(path, e))?,
            });
        }
    }

    Ok(files)
}
