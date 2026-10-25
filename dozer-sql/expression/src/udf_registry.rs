//! Per-application UDF definitions and an immutable startup snapshot of WASM modules.
//! There is no process-global cache; a new registry is required to reload modules.

use std::{
    collections::HashSet,
    ops::Deref,
    sync::{Arc, OnceLock},
};

use dozer_types::{
    models::udf_config::{UdfConfig, UdfType},
    thiserror,
};

#[derive(Debug, thiserror::Error)]
pub enum InitializationError {
    #[error("duplicate UDF name: {0}")]
    Duplicate(String),
    #[error("WASM UDF support requires the wasm Cargo feature")]
    Disabled,
    #[cfg(feature = "wasm")]
    #[error("cannot initialize WASM UDF {name}: {source}")]
    Wasm {
        name: String,
        source: crate::wasm::Error,
    },
}

#[derive(Debug, Default)]
struct Loaded {
    #[cfg(feature = "wasm")]
    wasm: std::collections::HashMap<String, Arc<crate::wasm::Compiled>>,
}

#[derive(Debug)]
struct Definitions {
    configs: Vec<UdfConfig>,
    loaded: OnceLock<Result<Loaded, Arc<InitializationError>>>,
}

/// Clones share the same module bytes, compilation and initialization outcome.
#[derive(Debug, Clone)]
pub struct UdfRegistry(Arc<Definitions>);

impl UdfRegistry {
    pub fn configs(&self) -> &[UdfConfig] {
        &self.0.configs
    }

    /// Load all configured WASM exports, including ones not referenced by SQL.
    /// A failure is retained; no partially initialized registry is published.
    pub fn prepare(&self) -> Result<(), crate::error::Error> {
        self.loaded().map(|_| ())
    }

    fn loaded(&self) -> Result<&Loaded, crate::error::Error> {
        self.0
            .loaded
            .get_or_init(|| self.initialize().map_err(Arc::new))
            .as_ref()
            .map_err(|error| crate::error::Error::UdfInitialization(error.clone()))
    }

    fn initialize(&self) -> Result<Loaded, InitializationError> {
        let mut names = HashSet::new();
        for config in self.configs() {
            if !names.insert(config.name.to_lowercase()) {
                return Err(InitializationError::Duplicate(config.name.clone()));
            }
        }
        #[allow(unused_mut)]
        let mut loaded = Loaded::default();
        for config in self.configs() {
            if let UdfType::Wasm(wasm) = &config.config {
                #[cfg(feature = "wasm")]
                {
                    let compiled = crate::wasm::Compiled::load(config.name.clone(), wasm).map_err(
                        |source| InitializationError::Wasm {
                            name: config.name.clone(),
                            source,
                        },
                    )?;
                    loaded.wasm.insert(config.name.clone(), compiled);
                }
                #[cfg(not(feature = "wasm"))]
                {
                    let _ = wasm;
                    return Err(InitializationError::Disabled);
                }
            }
        }
        Ok(loaded)
    }

    #[cfg(feature = "wasm")]
    pub(crate) fn wasm(
        &self,
        name: &str,
        config: &dozer_types::models::udf_config::WasmConfig,
    ) -> Result<Arc<crate::wasm::Compiled>, crate::error::Error> {
        // A caller cannot combine one registry with different raw configurations.
        if !self
            .configs()
            .iter()
            .any(|entry| entry.name == name && entry.config == UdfType::Wasm(config.clone()))
        {
            return Err(crate::wasm::Error::Config(format!(
                "{name} does not match the prepared registry"
            ))
            .into());
        }
        self.loaded()?
            .wasm
            .get(name)
            .cloned()
            .ok_or_else(|| crate::wasm::Error::Config(format!("{name} is not registered")).into())
    }
}

impl Default for UdfRegistry {
    fn default() -> Self {
        Vec::new().into()
    }
}
impl From<Vec<UdfConfig>> for UdfRegistry {
    fn from(configs: Vec<UdfConfig>) -> Self {
        Self(Arc::new(Definitions {
            configs,
            loaded: OnceLock::new(),
        }))
    }
}
impl From<&[UdfConfig]> for UdfRegistry {
    fn from(configs: &[UdfConfig]) -> Self {
        configs.to_vec().into()
    }
}
impl From<&Vec<UdfConfig>> for UdfRegistry {
    fn from(configs: &Vec<UdfConfig>) -> Self {
        configs.clone().into()
    }
}
impl<const N: usize> From<&[UdfConfig; N]> for UdfRegistry {
    fn from(configs: &[UdfConfig; N]) -> Self {
        configs.as_slice().into()
    }
}
impl From<&UdfRegistry> for UdfRegistry {
    fn from(registry: &UdfRegistry) -> Self {
        registry.clone()
    }
}
impl Deref for UdfRegistry {
    type Target = [UdfConfig];
    fn deref(&self) -> &Self::Target {
        self.configs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::models::udf_config::{JavaScriptConfig, OnnxConfig, WasmConfig};

    fn config(name: &str, path: &str) -> UdfConfig {
        UdfConfig {
            name: name.into(),
            config: UdfType::Wasm(WasmConfig { path: path.into() }),
        }
    }

    #[test]
    fn duplicate_names_are_rejected_before_loading_any_files() {
        let configs = vec![
            config("f", "does-not-exist.wasm"),
            UdfConfig {
                name: "F".into(),
                config: UdfType::Onnx(OnnxConfig {
                    path: "missing".into(),
                }),
            },
        ];
        let error = UdfRegistry::from(configs).prepare().unwrap_err();
        assert!(error.to_string().contains("duplicate UDF name: F"));
    }

    #[test]
    fn legacy_udfs_keep_their_existing_loading_behavior() {
        UdfRegistry::from(vec![
            UdfConfig {
                name: "model".into(),
                config: UdfType::Onnx(OnnxConfig {
                    path: "missing".into(),
                }),
            },
            UdfConfig {
                name: "script".into(),
                config: UdfType::JavaScript(JavaScriptConfig {
                    module: "missing".into(),
                }),
            },
        ])
        .prepare()
        .unwrap();
        UdfRegistry::default().prepare().unwrap();
    }

    #[test]
    #[cfg(not(feature = "wasm"))]
    fn unused_wasm_is_rejected_without_the_feature() {
        let error = UdfRegistry::from(vec![config("f", "missing")])
            .prepare()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("requires the wasm Cargo feature"));
    }

    #[cfg(feature = "wasm")]
    mod enabled {
        use super::*;
        use crate::{execution::Expression, wasm::Udf};
        use dozer_types::types::{Field, Record, Schema};

        fn write(path: &std::path::Path, wat: &str) {
            std::fs::write(path, wat::parse_str(wat).unwrap()).unwrap();
        }

        fn constant(value: i32) -> String {
            format!("(module (func (export \"f\") (result i32) i32.const {value}))")
        }

        fn value(registry: &UdfRegistry, wasm: &WasmConfig) -> Field {
            Udf::bind(
                registry.wasm("f", wasm).unwrap(),
                vec![],
                &Schema::default(),
            )
            .unwrap()
            .evaluate(&Record::new(vec![]), &Schema::default())
            .unwrap()
        }

        #[test]
        fn snapshots_survive_file_changes_and_are_isolated_between_applications() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("f.wasm");
            write(&path, &constant(7));
            let wasm = WasmConfig {
                path: path.to_str().unwrap().into(),
            };
            let definitions = vec![config("f", &wasm.path)];
            let first = UdfRegistry::from(definitions.clone());
            first.prepare().unwrap();
            let clone = first.clone();

            write(&path, &constant(9));
            let second = UdfRegistry::from(definitions);
            second.prepare().unwrap();
            std::fs::remove_file(&path).unwrap();

            assert_eq!(value(&first, &wasm), Field::Int(7));
            assert_eq!(value(&clone, &wasm), Field::Int(7));
            assert_eq!(value(&second, &wasm), Field::Int(9));
            assert!(Arc::ptr_eq(
                &first.wasm("f", &wasm).unwrap(),
                &clone.wasm("f", &wasm).unwrap()
            ));
            assert!(!Arc::ptr_eq(
                &first.wasm("f", &wasm).unwrap(),
                &second.wasm("f", &wasm).unwrap()
            ));
            assert!(first
                .wasm(
                    "f",
                    &WasmConfig {
                        path: "different.wasm".into()
                    }
                )
                .is_err());
        }

        #[test]
        fn concurrent_preparation_publishes_one_complete_snapshot() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("f.wasm");
            write(&path, &constant(7));
            let wasm = WasmConfig {
                path: path.to_str().unwrap().into(),
            };
            let registry = UdfRegistry::from(vec![config("f", &wasm.path)]);
            let barrier = std::sync::Barrier::new(8);
            let compiled = std::thread::scope(|scope| {
                let tasks = (0..8)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            registry.prepare().unwrap();
                            registry.wasm("f", &wasm).unwrap()
                        })
                    })
                    .collect::<Vec<_>>();
                tasks
                    .into_iter()
                    .map(|task| task.join().unwrap())
                    .collect::<Vec<_>>()
            });
            assert!(compiled
                .iter()
                .all(|module| Arc::ptr_eq(module, &compiled[0])));
            std::fs::remove_file(&path).unwrap();
            assert_eq!(value(&registry, &wasm), Field::Int(7));
        }

        #[test]
        fn failed_startup_is_cached_without_publishing_partial_modules() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("f.wasm");
            write(&path, &constant(7));
            let missing = dir.path().join("other.wasm");
            let wasm = WasmConfig {
                path: path.to_str().unwrap().into(),
            };
            let definitions = vec![
                config("f", &wasm.path),
                config("g", missing.to_str().unwrap()),
            ];
            let registry = UdfRegistry::from(definitions.clone());
            let error = registry.prepare().unwrap_err().to_string();
            assert!(error.contains("cannot initialize WASM UDF g"));
            write(
                &missing,
                "(module (func (export \"g\") (result i32) i32.const 1))",
            );
            assert_eq!(registry.clone().prepare().unwrap_err().to_string(), error);
            assert!(registry.wasm("f", &wasm).is_err());
            UdfRegistry::from(definitions).prepare().unwrap();
        }

        #[test]
        fn startup_checks_unused_modules_exports_imports_signatures_and_initialization() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("f.wasm");
            for (wat, message) in [
                ("(module)", "export f must be a function"),
                ("(module (func (export \"f\")))", "Unsupported WASM signature"),
                ("(module (import \"env\" \"other\" (func)) (func (export \"f\") (result i32) i32.const 0))", "import is not allowed"),
                ("(module (func $start unreachable) (start $start) (func (export \"f\") (result i32) i32.const 0))", "execution failed"),
                ("(module (memory 257) (func (export \"f\") (result i32) i32.const 0))", "execution failed"),
            ] {
                write(&path, wat);
                let error = UdfRegistry::from(vec![config("f", path.to_str().unwrap())]).prepare().unwrap_err();
                assert!(error.to_string().contains(message), "{error}");
            }
            std::fs::write(&path, b"not wasm").unwrap();
            assert!(UdfRegistry::from(vec![config("f", path.to_str().unwrap())])
                .prepare()
                .unwrap_err()
                .to_string()
                .contains("Invalid WASM module"));
        }

        #[test]
        fn builtin_names_are_rejected_before_file_access() {
            for name in ["abs", "sum", "coalesce", "now", "py_custom"] {
                assert!(
                    UdfRegistry::from(vec![config(name, "missing")])
                        .prepare()
                        .unwrap_err()
                        .to_string()
                        .contains("conflicts with a built-in SQL function"),
                    "{name}"
                );
            }
        }

        #[test]
        fn multiple_expressions_reuse_compilation_but_not_mutable_guest_state() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("f.wasm");
            write(&path, "(module (global $g (mut i32) (i32.const 0)) (func (export \"f\") (param i32) (result i32) global.get $g local.get 0 i32.add global.set $g global.get $g))");
            let wasm = WasmConfig {
                path: path.to_str().unwrap().into(),
            };
            let registry = UdfRegistry::from(vec![config("f", &wasm.path)]);
            registry.prepare().unwrap();
            std::fs::remove_file(&path).unwrap();
            for number in [7, 9, 7] {
                let mut function = Udf::bind(
                    registry.wasm("f", &wasm).unwrap(),
                    vec![Expression::Literal(Field::Int(number))],
                    &Schema::default(),
                )
                .unwrap();
                for _ in 0..3 {
                    assert_eq!(
                        function
                            .evaluate(&Record::new(vec![]), &Schema::default())
                            .unwrap(),
                        Field::Int(number)
                    );
                }
            }
        }
    }
}
