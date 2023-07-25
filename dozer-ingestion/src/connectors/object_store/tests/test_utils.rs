use dozer_types::ingestion_types::{
    CsvConfig, LocalDetails, LocalStorage, ParquetConfig, Table, TableConfig,
};
use std::path::PathBuf;

pub fn get_local_storage_config(typ: &str, prefix: &str) -> LocalStorage {
    let p = PathBuf::from("src/connectors/object_store/tests/files".to_string());
    match typ {
        "parquet" => match prefix {
            "" => LocalStorage {
                details: Some(LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                }),
                tables: vec![Table {
                    config: Some(TableConfig::Parquet(ParquetConfig {
                        extension: typ.to_string(),
                        path: format!("all_types_{typ}"),
                        marker_file: false,
                        marker_extension: String::new(),
                    })),
                    name: format!("all_types_{typ}"),
                }],
            },
            &_ => LocalStorage {
                details: Some(LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                }),
                tables: vec![Table {
                    config: Some(TableConfig::Parquet(ParquetConfig {
                        extension: typ.to_string(),
                        path: format!("{prefix}_{typ}"),
                        marker_file: true,
                        marker_extension: String::from(".marker"),
                    })),
                    name: format!("{prefix}_{typ}"),
                }],
            },
        },
        "csv" => match prefix {
            "" => LocalStorage {
                details: Some(LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                }),
                tables: vec![Table {
                    config: Some(TableConfig::CSV(CsvConfig {
                        extension: typ.to_string(),
                        path: format!("all_types_{typ}"),
                        marker_file: false,
                        marker_extension: String::new(),
                    })),
                    name: format!("all_types_{typ}"),
                }],
            },
            &_ => LocalStorage {
                details: Some(LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                }),
                tables: vec![Table {
                    config: Some(TableConfig::CSV(CsvConfig {
                        extension: typ.to_string(),
                        path: format!("{prefix}_{typ}"),
                        marker_file: true,
                        marker_extension: String::from(".marker"),
                    })),
                    name: format!("{prefix}_{typ}"),
                }],
            },
        },
        &_ => LocalStorage {
            details: Some(LocalDetails {
                path: p.to_str().unwrap().to_string(),
            }),
            tables: vec![Table {
                config: None,
                name: String::new(),
            }],
        },
    }
}
