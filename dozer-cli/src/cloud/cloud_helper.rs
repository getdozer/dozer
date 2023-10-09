use crate::errors::ConfigCombineError::{
    CannotReadConfig, CannotReadFile, WrongPatternOfConfigFilesGlob,
};

use dozer_types::grpc_types::cloud::File;
use glob::glob;

pub fn list_files(config_paths: Vec<String>) -> Result<Vec<File>, crate::errors::CloudError> {
    let mut files = vec![];
    for pattern in config_paths {
        let files_glob = glob(&pattern).map_err(WrongPatternOfConfigFilesGlob)?;

        for entry in files_glob {
            let path = entry.map_err(CannotReadFile)?;
            let content =
                std::fs::read(path.clone()).map_err(|e| CannotReadConfig(path.clone(), e))?;

            files.push(File {
                name: path.clone().to_str().unwrap().to_string(),
                content,
            });
        }
    }

    Ok(files)
}
