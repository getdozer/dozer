use dozer_types::log::{self, debug};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::fs;

use deno_ast::MediaType;
use deno_cache_dir::{GlobalToLocalCopy, HttpCache};
use deno_core::{
    self,
    error::{custom_error, generic_error, uri_error, AnyError},
    futures::{self, FutureExt as _},
    parking_lot::Mutex,
    ModuleSpecifier,
};
use deno_fetch::{
    data_url::DataUrl,
    reqwest::{
        header::{HeaderValue, ACCEPT, IF_NONE_MATCH},
        StatusCode, Url,
    },
};
use deno_terminal::colors;
use deno_web::BlobStore;

use super::{
    http_util::{resolve_redirect_from_response, HeadersMap, HttpClient},
    text_encoding,
};
use crate::runtime::permissions::PermissionsContainer;

pub const SUPPORTED_SCHEMES: [&str; 5] = ["data", "blob", "file", "http", "https"];

/// A structure representing a source file.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct File {
    /// For remote files, if there was an `X-TypeScript-Type` header, the parsed
    /// out value of that header.
    pub maybe_types: Option<String>,
    /// The resolved media type for the file.
    pub media_type: MediaType,
    /// The source of the file as a string.
    pub source: Arc<str>,
    /// The _final_ specifier for the file.  The requested specifier and the final
    /// specifier maybe different for remote files that have been redirected.
    pub specifier: ModuleSpecifier,

    pub maybe_headers: Option<HashMap<String, String>>,
}

/// Simple struct implementing in-process caching to prevent multiple
/// fs reads/net fetches for same file.
#[derive(Debug, Clone, Default)]
struct FileCache(Arc<Mutex<HashMap<ModuleSpecifier, File>>>);

impl FileCache {
    pub fn get(&self, specifier: &ModuleSpecifier) -> Option<File> {
        let cache = self.0.lock();
        cache.get(specifier).cloned()
    }

    pub fn insert(&self, specifier: ModuleSpecifier, file: File) -> Option<File> {
        let mut cache = self.0.lock();
        cache.insert(specifier, file)
    }
}

/// Fetch a source file from the local file system.
async fn fetch_local(specifier: &ModuleSpecifier) -> Result<File, AnyError> {
    let local = specifier
        .to_file_path()
        .map_err(|_| uri_error(format!("Invalid file path.\n  Specifier: {specifier}")))?;
    let bytes = fs::read(local).await?;
    let charset = text_encoding::detect_charset(&bytes).to_string();
    let source = get_source_from_bytes(bytes, Some(charset))?;
    let media_type = MediaType::from_specifier(specifier);

    Ok(File {
        maybe_types: None,
        media_type,
        source: source.into(),
        specifier: specifier.clone(),
        maybe_headers: None,
    })
}

/// Returns the decoded body and content-type of a provided
/// data URL.
pub fn get_source_from_data_url(specifier: &ModuleSpecifier) -> Result<(String, String), AnyError> {
    let data_url = DataUrl::process(specifier.as_str()).map_err(|e| uri_error(format!("{e:?}")))?;
    let mime = data_url.mime_type();
    let charset = mime.get_parameter("charset").map(|v| v.to_string());
    let (bytes, _) = data_url
        .decode_to_vec()
        .map_err(|e| uri_error(format!("{e:?}")))?;
    Ok((get_source_from_bytes(bytes, charset)?, format!("{mime}")))
}

/// Given a vector of bytes and optionally a charset, decode the bytes to a
/// string.
pub fn get_source_from_bytes(
    bytes: Vec<u8>,
    maybe_charset: Option<String>,
) -> Result<String, AnyError> {
    let source = if let Some(charset) = maybe_charset {
        text_encoding::convert_to_utf8(&bytes, &charset)?.to_string()
    } else {
        String::from_utf8(bytes)?
    };

    Ok(source)
}

/// Return a validated scheme for a given module specifier.
fn get_validated_scheme(specifier: &ModuleSpecifier) -> Result<String, AnyError> {
    let scheme = specifier.scheme();
    if !SUPPORTED_SCHEMES.contains(&scheme) {
        Err(generic_error(format!(
        "Unsupported scheme \"{scheme}\" for module \"{specifier}\". Supported schemes: {SUPPORTED_SCHEMES:#?}"
      )))
    } else {
        Ok(scheme.to_string())
    }
}

/// Resolve a media type and optionally the charset from a module specifier and
/// the value of a content type header.
pub fn map_content_type(
    specifier: &ModuleSpecifier,
    maybe_content_type: Option<&String>,
) -> (MediaType, Option<String>) {
    if let Some(content_type) = maybe_content_type {
        let mut content_types = content_type.split(';');
        let content_type = content_types.next().unwrap();
        let media_type = MediaType::from_content_type(specifier, content_type);
        let charset = content_types
            .map(str::trim)
            .find_map(|s| s.strip_prefix("charset="))
            .map(String::from);

        (media_type, charset)
    } else {
        (MediaType::from_specifier(specifier), None)
    }
}

pub struct FetchOptions<'a> {
    pub specifier: &'a ModuleSpecifier,
    pub permissions: PermissionsContainer,
    pub maybe_accept: Option<&'a str>,
}

/// A structure for resolving, fetching and caching source files.
#[derive(Debug, Clone)]
pub struct FileFetcher {
    allow_remote: bool,
    cache: FileCache,
    http_cache: Arc<dyn HttpCache>,
    http_client: Arc<HttpClient>,
    blob_store: Arc<BlobStore>,
    download_log_level: log::Level,
}

impl FileFetcher {
    pub fn new(
        http_cache: Arc<dyn HttpCache>,
        allow_remote: bool,
        http_client: Arc<HttpClient>,
        blob_store: Arc<BlobStore>,
    ) -> Self {
        Self {
            allow_remote,
            cache: Default::default(),
            http_cache,
            http_client,
            blob_store,
            download_log_level: log::Level::Info,
        }
    }

    /// Creates a `File` structure for a remote file.
    fn build_remote_file(
        &self,
        specifier: &ModuleSpecifier,
        bytes: Vec<u8>,
        headers: &HashMap<String, String>,
    ) -> Result<File, AnyError> {
        let maybe_content_type = headers.get("content-type");
        let (media_type, maybe_charset) = map_content_type(specifier, maybe_content_type);
        let source = get_source_from_bytes(bytes, maybe_charset)?;
        let maybe_types = match media_type {
            MediaType::JavaScript | MediaType::Cjs | MediaType::Mjs | MediaType::Jsx => {
                headers.get("x-typescript-types").cloned()
            }
            _ => None,
        };

        Ok(File {
            maybe_types,
            media_type,
            source: source.into(),
            specifier: specifier.clone(),
            maybe_headers: Some(headers.clone()),
        })
    }

    /// Fetch cached remote file.
    ///
    /// This is a recursive operation if source file has redirections.
    pub fn fetch_cached(
        &self,
        specifier: &ModuleSpecifier,
        redirect_limit: i64,
    ) -> Result<Option<File>, AnyError> {
        debug!("FileFetcher::fetch_cached - specifier: {}", specifier);
        if redirect_limit < 0 {
            return Err(custom_error("Http", "Too many redirects."));
        }

        let cache_key = self.http_cache.cache_item_key(specifier)?; // compute this once
        let Some(headers) = self.http_cache.read_headers(&cache_key)? else {
            return Ok(None);
        };
        if let Some(redirect_to) = headers.get("location") {
            let redirect = deno_core::resolve_import(redirect_to, specifier.as_str())?;
            return self.fetch_cached(&redirect, redirect_limit - 1);
        }
        let Some(bytes) =
            self.http_cache
                .read_file_bytes(&cache_key, None, GlobalToLocalCopy::Allow)?
        else {
            return Ok(None);
        };
        let file = self.build_remote_file(specifier, bytes, &headers)?;

        Ok(Some(file))
    }

    /// Convert a data URL into a file, resulting in an error if the URL is
    /// invalid.
    fn fetch_data_url(&self, specifier: &ModuleSpecifier) -> Result<File, AnyError> {
        debug!("FileFetcher::fetch_data_url() - specifier: {}", specifier);
        let (source, content_type) = get_source_from_data_url(specifier)?;
        let (media_type, _) = map_content_type(specifier, Some(&content_type));
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), content_type);
        Ok(File {
            maybe_types: None,
            media_type,
            source: source.into(),
            specifier: specifier.clone(),
            maybe_headers: Some(headers),
        })
    }

    /// Get a blob URL.
    async fn fetch_blob_url(&self, specifier: &ModuleSpecifier) -> Result<File, AnyError> {
        debug!("FileFetcher::fetch_blob_url() - specifier: {}", specifier);
        let blob = self
            .blob_store
            .get_object_url(specifier.clone())
            .ok_or_else(|| {
                custom_error("NotFound", format!("Blob URL not found: \"{specifier}\"."))
            })?;

        let content_type = blob.media_type.clone();
        let bytes = blob.read_all().await?;

        let (media_type, maybe_charset) = map_content_type(specifier, Some(&content_type));
        let source = get_source_from_bytes(bytes, maybe_charset)?;
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), content_type);

        Ok(File {
            maybe_types: None,
            media_type,
            source: source.into(),
            specifier: specifier.clone(),
            maybe_headers: Some(headers),
        })
    }

    /// Asynchronously fetch remote source file specified by the URL following
    /// redirects.
    ///
    /// **Note** this is a recursive method so it can't be "async", but needs to
    /// return a `Pin<Box<..>>`.
    fn fetch_remote(
        &self,
        specifier: &ModuleSpecifier,
        permissions: PermissionsContainer,
        redirect_limit: i64,
        maybe_accept: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<File, AnyError>> + Send>> {
        debug!("FileFetcher::fetch_remote() - specifier: {}", specifier);
        if redirect_limit < 0 {
            return futures::future::err(custom_error("Http", "Too many redirects.")).boxed();
        }

        if let Err(err) = permissions.check_specifier(specifier) {
            return futures::future::err(err).boxed();
        }

        match self.fetch_cached(specifier, redirect_limit) {
            Ok(Some(file)) => {
                return futures::future::ok(file).boxed();
            }
            Ok(None) => {}
            Err(err) => {
                return futures::future::err(err).boxed();
            }
        }

        log::log!(
            self.download_log_level,
            "{} {}",
            colors::green("Download"),
            specifier
        );

        let maybe_etag = self
            .http_cache
            .cache_item_key(specifier)
            .ok()
            .and_then(|key| self.http_cache.read_headers(&key).ok().flatten())
            .and_then(|headers| headers.get("etag").cloned());
        let specifier = specifier.clone();
        let client = self.http_client.clone();
        let file_fetcher = self.clone();
        // A single pass of fetch either yields code or yields a redirect, server
        // error causes a single retry to avoid crashing hard on intermittent failures.

        async fn handle_request_or_server_error(
            retried: &mut bool,
            specifier: &Url,
            err_str: String,
        ) -> Result<(), AnyError> {
            // Retry once, and bail otherwise.
            if !*retried {
                *retried = true;
                log::debug!("Import '{}' failed: {}. Retrying...", specifier, err_str);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Ok(())
            } else {
                Err(generic_error(format!(
                    "Import '{}' failed: {}",
                    specifier, err_str
                )))
            }
        }

        async move {
            let mut retried = false;
            loop {
                let result = match fetch_once(
                    &client,
                    FetchOnceArgs {
                        url: specifier.clone(),
                        maybe_accept: maybe_accept.clone(),
                        maybe_etag: maybe_etag.clone(),
                    },
                )
                .await?
                {
                    FetchOnceResult::NotModified => {
                        let file = file_fetcher.fetch_cached(&specifier, 10)?.unwrap();
                        Ok(file)
                    }
                    FetchOnceResult::Redirect(redirect_url, headers) => {
                        file_fetcher.http_cache.set(&specifier, headers, &[])?;
                        file_fetcher
                            .fetch_remote(
                                &redirect_url,
                                permissions,
                                redirect_limit - 1,
                                maybe_accept,
                            )
                            .await
                    }
                    FetchOnceResult::Code(bytes, headers) => {
                        file_fetcher
                            .http_cache
                            .set(&specifier, headers.clone(), &bytes)?;
                        let file = file_fetcher.build_remote_file(&specifier, bytes, &headers)?;
                        Ok(file)
                    }
                    FetchOnceResult::RequestError(err) => {
                        handle_request_or_server_error(&mut retried, &specifier, err).await?;
                        continue;
                    }
                    FetchOnceResult::ServerError(status) => {
                        handle_request_or_server_error(
                            &mut retried,
                            &specifier,
                            status.to_string(),
                        )
                        .await?;
                        continue;
                    }
                };
                break result;
            }
        }
        .boxed()
    }

    /// Fetch a source file and asynchronously return it.
    pub async fn fetch(
        &self,
        specifier: &ModuleSpecifier,
        permissions: PermissionsContainer,
    ) -> Result<File, AnyError> {
        self.fetch_with_options(FetchOptions {
            specifier,
            permissions,
            maybe_accept: None,
        })
        .await
    }

    pub async fn fetch_with_options(&self, options: FetchOptions<'_>) -> Result<File, AnyError> {
        let specifier = options.specifier;
        debug!("FileFetcher::fetch() - specifier: {}", specifier);
        let scheme = get_validated_scheme(specifier)?;
        options.permissions.check_specifier(specifier)?;
        if let Some(file) = self.cache.get(specifier) {
            Ok(file)
        } else if scheme == "file" {
            // we do not in memory cache files, as this would prevent files on the
            // disk changing effecting things like workers and dynamic imports.
            fetch_local(specifier).await
        } else if scheme == "data" {
            self.fetch_data_url(specifier)
        } else if scheme == "blob" {
            self.fetch_blob_url(specifier).await
        } else if !self.allow_remote {
            Err(custom_error(
        "NoRemote",
        format!("A remote specifier was requested: \"{specifier}\", but --no-remote is specified."),
      ))
        } else {
            let result = self
                .fetch_remote(
                    specifier,
                    options.permissions,
                    10,
                    options.maybe_accept.map(String::from),
                )
                .await;
            if let Ok(file) = &result {
                self.cache.insert(specifier.clone(), file.clone());
            }
            result
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum FetchOnceResult {
    Code(Vec<u8>, HeadersMap),
    NotModified,
    Redirect(Url, HeadersMap),
    RequestError(String),
    ServerError(StatusCode),
}

#[derive(Debug)]
struct FetchOnceArgs {
    pub url: Url,
    pub maybe_accept: Option<String>,
    pub maybe_etag: Option<String>,
}

/// Asynchronously fetches the given HTTP URL one pass only.
/// If no redirect is present and no error occurs,
/// yields Code(ResultPayload).
/// If redirect occurs, does not follow and
/// yields Redirect(url).
async fn fetch_once(
    http_client: &HttpClient,
    args: FetchOnceArgs,
) -> Result<FetchOnceResult, AnyError> {
    let mut request = http_client.get_no_redirect(args.url.clone())?;

    if let Some(etag) = args.maybe_etag {
        let if_none_match_val = HeaderValue::from_str(&etag)?;
        request = request.header(IF_NONE_MATCH, if_none_match_val);
    }
    if let Some(accept) = args.maybe_accept {
        let accepts_val = HeaderValue::from_str(&accept)?;
        request = request.header(ACCEPT, accepts_val);
    }
    let response = match request.send().await {
        Ok(resp) => resp,
        Err(err) => {
            if err.is_connect() || err.is_timeout() {
                return Ok(FetchOnceResult::RequestError(err.to_string()));
            }
            return Err(err.into());
        }
    };

    if response.status() == StatusCode::NOT_MODIFIED {
        return Ok(FetchOnceResult::NotModified);
    }

    let mut result_headers = HashMap::new();
    let response_headers = response.headers();

    if let Some(warning) = response_headers.get("X-Deno-Warning") {
        log::warn!(
            "{} {}",
            colors::yellow("Warning"),
            warning.to_str().unwrap()
        );
    }

    for key in response_headers.keys() {
        let key_str = key.to_string();
        let values = response_headers.get_all(key);
        let values_str = values
            .iter()
            .map(|e| e.to_str().unwrap().to_string())
            .collect::<Vec<String>>()
            .join(",");
        result_headers.insert(key_str, values_str);
    }

    if response.status().is_redirection() {
        let new_url = resolve_redirect_from_response(&args.url, &response)?;
        return Ok(FetchOnceResult::Redirect(new_url, result_headers));
    }

    let status = response.status();

    if status.is_server_error() {
        return Ok(FetchOnceResult::ServerError(status));
    }

    if status.is_client_error() {
        let err = if response.status() == StatusCode::NOT_FOUND {
            custom_error(
                "NotFound",
                format!("Import '{}' failed, not found.", args.url),
            )
        } else {
            generic_error(format!(
                "Import '{}' failed: {}",
                args.url,
                response.status()
            ))
        };
        return Err(err);
    }

    let body = response.bytes().await?.into();

    Ok(FetchOnceResult::Code(body, result_headers))
}
