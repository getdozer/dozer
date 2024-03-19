// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

use std::path::Path;

use deno_core::error::AnyError;
use deno_core::url::Url;

// NOTE: Temporary permissions container to satisfy traits. We are migrating to the deno_permissions
// crate.
#[derive(Debug, Clone)]
pub struct PermissionsContainer(pub deno_permissions::PermissionsContainer);

impl PermissionsContainer {
    pub fn allow_all() -> Self {
        Self(deno_permissions::PermissionsContainer::allow_all())
    }
}

impl std::ops::Deref for PermissionsContainer {
    type Target = deno_permissions::PermissionsContainer;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for PermissionsContainer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl deno_fetch::FetchPermissions for PermissionsContainer {
    #[inline(always)]
    fn check_net_url(&mut self, url: &Url, api_name: &str) -> Result<(), AnyError> {
        self.0.check_net_url(url, api_name)
    }

    #[inline(always)]
    fn check_read(&mut self, path: &Path, api_name: &str) -> Result<(), AnyError> {
        self.0.check_read(path, api_name)
    }
}

impl deno_web::TimersPermission for PermissionsContainer {
    #[inline(always)]
    fn allow_hrtime(&mut self) -> bool {
        self.0.allow_hrtime()
    }
}

impl deno_websocket::WebSocketPermissions for PermissionsContainer {
    #[inline(always)]
    fn check_net_url(&mut self, url: &Url, api_name: &str) -> Result<(), AnyError> {
        self.0.check_net_url(url, api_name)
    }
}

// NOTE(bartlomieju): for now, NAPI uses `--allow-ffi` flag, but that might
// change in the future.
impl deno_napi::NapiPermissions for PermissionsContainer {
    #[inline(always)]
    fn check(&mut self, path: Option<&Path>) -> Result<(), AnyError> {
        self.0.check_ffi(path)
    }
}
