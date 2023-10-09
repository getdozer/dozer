use crate::errors::ConfigCombineError::{
    CannotReadConfig, CannotReadFile, WrongPatternOfConfigFilesGlob,
};

use dozer_types::grpc_types::cloud::File;
use glob::glob;
use std::fs;
use std::io::{BufReader, Read};

pub fn list_files(config_paths: Vec<String>) -> Result<Vec<File>, crate::errors::CloudError> {
    let mut files = vec![];
    for pattern in config_paths {
        let files_glob = glob(&pattern).map_err(WrongPatternOfConfigFilesGlob)?;

        for entry in files_glob {
            let path = entry.map_err(CannotReadFile)?;
            let f = fs::File::open(path.clone()).map_err(|e| CannotReadConfig(path.clone(), e))?;
            let mut reader = BufReader::new(f);
            let mut buffer = Vec::new();

            reader
                .read_to_end(&mut buffer)
                .map_err(|e| CannotReadConfig(path.clone(), e))?;

            files.push(File {
                name: path.clone().to_str().unwrap().to_string(),
                content: buffer,
            });
        }
    }

    Ok(files)
}
