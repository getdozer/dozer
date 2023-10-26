use dozer_ingestion_connector::dozer_types::models::ingestion_types::{
    CsvConfig, LocalDetails, LocalStorage, ParquetConfig, Table, TableConfig,
};
use std::path::PathBuf;

pub fn get_local_storage_config(typ: &str, prefix: &str) -> LocalStorage {
    let p = PathBuf::from("src/tests/files".to_string());
    match typ {
        "parquet" => match prefix {
            "" => LocalStorage {
                details: LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                },
                tables: vec![Table {
                    config: TableConfig::Parquet(ParquetConfig {
                        extension: typ.to_string(),
                        path: format!("all_types_{typ}"),
                        marker_extension: None,
                    }),
                    name: format!("all_types_{typ}"),
                }],
            },
            &_ => LocalStorage {
                details: LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                },
                tables: vec![Table {
                    config: TableConfig::Parquet(ParquetConfig {
                        extension: typ.to_string(),
                        path: format!("{prefix}_{typ}"),
                        marker_extension: Some(String::from(".marker")),
                    }),
                    name: format!("{prefix}_{typ}"),
                }],
            },
        },
        "csv" => match prefix {
            "" => LocalStorage {
                details: LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                },
                tables: vec![Table {
                    config: TableConfig::CSV(CsvConfig {
                        extension: typ.to_string(),
                        path: format!("all_types_{typ}"),
                        marker_extension: None,
                    }),
                    name: format!("all_types_{typ}"),
                }],
            },
            &_ => LocalStorage {
                details: LocalDetails {
                    path: p.to_str().unwrap().to_string(),
                },
                tables: vec![Table {
                    config: TableConfig::CSV(CsvConfig {
                        extension: typ.to_string(),
                        path: format!("{prefix}_{typ}"),
                        marker_extension: Some(String::from(".marker")),
                    }),
                    name: format!("{prefix}_{typ}"),
                }],
            },
        },
        other => panic!("Unsupported type: {}", other),
    }
}
