// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.
//! This example shows how to use swc to transpile TypeScript and JSX/TSX
//! modules.
//!
//! It will only transpile, not typecheck (like Deno's `--no-check` flag).

use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use deno_runtime::deno_core::{self, anyhow, futures};

use anyhow::bail;
use anyhow::Error;
use deno_ast::MediaType;
use deno_ast::ParseParams;
use deno_ast::SourceTextInfo;
use deno_core::error::AnyError;
use deno_core::resolve_import;
use deno_core::ModuleLoader;
use deno_core::ModuleSource;
use deno_core::ModuleSourceFuture;
use deno_core::ModuleSpecifier;
use deno_core::ModuleType;
use deno_core::ResolutionKind;
use deno_core::SourceMapGetter;
use deno_runtime::permissions::PermissionsContainer;
use futures::FutureExt;

use self::cache::GlobalHttpCache;
use self::cache::RealDenoCacheEnv;
use self::file_fetcher::FileFetcher;
use self::http_util::HttpClient;

use tempdir::TempDir;

#[derive(Clone)]
struct SourceMapStore(Rc<RefCell<HashMap<String, Vec<u8>>>>);

impl SourceMapGetter for SourceMapStore {
    fn get_source_map(&self, specifier: &str) -> Option<Vec<u8>> {
        self.0.borrow().get(specifier).cloned()
    }

    fn get_source_line(&self, _file_name: &str, _line_number: usize) -> Option<String> {
        None
    }
}

pub struct TypescriptModuleLoader {
    source_maps: SourceMapStore,
    file_fetcher: FileFetcher,
    _temp_dir: TempDir,
}

impl TypescriptModuleLoader {
    pub fn new() -> Result<Self, std::io::Error> {
        let temp_dir = TempDir::new("dozer-deno-cache")?;
        Ok(Self {
            source_maps: SourceMapStore(Rc::new(RefCell::new(HashMap::new()))),
            file_fetcher: FileFetcher::new(
                Arc::new(GlobalHttpCache::new(
                    temp_dir.path().to_path_buf(),
                    RealDenoCacheEnv,
                )),
                true,
                Arc::new(HttpClient::new(None, None)),
                Default::default(),
            ),
            _temp_dir: temp_dir,
        })
    }
}

impl ModuleLoader for TypescriptModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _kind: ResolutionKind,
    ) -> Result<ModuleSpecifier, Error> {
        Ok(resolve_import(specifier, referrer)?)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&ModuleSpecifier>,
        _is_dyn_import: bool,
    ) -> Pin<Box<ModuleSourceFuture>> {
        let source_maps = self.source_maps.clone();
        async fn load(
            source_maps: SourceMapStore,
            file_fetcher: FileFetcher,
            module_specifier: ModuleSpecifier,
        ) -> Result<ModuleSource, AnyError> {
            let media_type = MediaType::from_specifier(&module_specifier);
            let (module_type, should_transpile) = match media_type {
                MediaType::JavaScript | MediaType::Mjs | MediaType::Cjs => {
                    (ModuleType::JavaScript, false)
                }
                MediaType::Jsx => (ModuleType::JavaScript, true),
                MediaType::TypeScript
                | MediaType::Mts
                | MediaType::Cts
                | MediaType::Dts
                | MediaType::Dmts
                | MediaType::Dcts
                | MediaType::Tsx => (ModuleType::JavaScript, true),
                MediaType::Json => (ModuleType::Json, false),
                _ => bail!("Unknown media type: {:?}", module_specifier),
            };

            let code = file_fetcher
                .fetch(&module_specifier, PermissionsContainer::allow_all())
                .await?
                .source
                .to_string();
            let code = if should_transpile {
                let parsed = deno_ast::parse_module(ParseParams {
                    specifier: module_specifier.to_string(),
                    text_info: SourceTextInfo::from_string(code),
                    media_type,
                    capture_tokens: false,
                    scope_analysis: false,
                    maybe_syntax: None,
                })?;
                let res = parsed.transpile(&deno_ast::EmitOptions {
                    inline_source_map: false,
                    source_map: true,
                    inline_sources: true,
                    ..Default::default()
                })?;
                let source_map = res.source_map.unwrap();
                source_maps
                    .0
                    .borrow_mut()
                    .insert(module_specifier.to_string(), source_map.into_bytes());
                res.text
            } else {
                code
            };
            Ok(ModuleSource::new(
                module_type,
                code.into(),
                &module_specifier,
            ))
        }

        load(
            source_maps,
            self.file_fetcher.clone(),
            module_specifier.clone(),
        )
        .boxed_local()
    }
}

mod cache;
mod file_fetcher;
mod fs;
mod http_util;
mod text_encoding;
