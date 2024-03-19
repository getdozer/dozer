use std::{collections::HashMap, sync::Arc};

use deno_core::error::{generic_error, AnyError};
use deno_fetch::{
    create_http_client,
    reqwest::{self, header::LOCATION, Response, Url},
    CreateHttpClientOptions,
};
use deno_tls::RootCertStoreProvider;
use dozer_types::log;

use crate::user_agent;

/// Construct the next uri based on base uri and location header fragment
/// See <https://tools.ietf.org/html/rfc3986#section-4.2>
fn resolve_url_from_location(base_url: &Url, location: &str) -> Url {
    if location.starts_with("http://") || location.starts_with("https://") {
        // absolute uri
        Url::parse(location).expect("provided redirect url should be a valid url")
    } else if location.starts_with("//") {
        // "//" authority path-abempty
        Url::parse(&format!("{}:{}", base_url.scheme(), location))
            .expect("provided redirect url should be a valid url")
    } else if location.starts_with('/') {
        // path-absolute
        base_url
            .join(location)
            .expect("provided redirect url should be a valid url")
    } else {
        // assuming path-noscheme | path-empty
        let base_url_path_str = base_url.path().to_owned();
        // Pop last part or url (after last slash)
        let segs: Vec<&str> = base_url_path_str.rsplitn(2, '/').collect();
        let new_path = format!("{}/{}", segs.last().unwrap_or(&""), location);
        base_url
            .join(&new_path)
            .expect("provided redirect url should be a valid url")
    }
}

pub fn resolve_redirect_from_response(
    request_url: &Url,
    response: &Response,
) -> Result<Url, AnyError> {
    debug_assert!(response.status().is_redirection());
    if let Some(location) = response.headers().get(LOCATION) {
        let location_string = location.to_str()?;
        log::debug!("Redirecting to {:?}...", &location_string);
        let new_url = resolve_url_from_location(request_url, location_string);
        Ok(new_url)
    } else {
        Err(generic_error(format!(
            "Redirection from '{request_url}' did not provide location header"
        )))
    }
}

// TODO(ry) HTTP headers are not unique key, value pairs. There may be more than
// one header line with the same key. This should be changed to something like
// Vec<(String, String)>
pub type HeadersMap = HashMap<String, String>;

pub struct HttpClient {
    options: CreateHttpClientOptions,
    root_cert_store_provider: Option<Arc<dyn RootCertStoreProvider>>,
    cell: once_cell::sync::OnceCell<reqwest::Client>,
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("options", &self.options)
            .finish()
    }
}

impl HttpClient {
    pub fn new(
        root_cert_store_provider: Option<Arc<dyn RootCertStoreProvider>>,
        unsafely_ignore_certificate_errors: Option<Vec<String>>,
    ) -> Self {
        Self {
            options: CreateHttpClientOptions {
                unsafely_ignore_certificate_errors,
                ..Default::default()
            },
            root_cert_store_provider,
            cell: Default::default(),
        }
    }

    fn client(&self) -> Result<&reqwest::Client, AnyError> {
        self.cell.get_or_try_init(|| {
            create_http_client(
                &user_agent(),
                CreateHttpClientOptions {
                    root_cert_store: match &self.root_cert_store_provider {
                        Some(provider) => Some(provider.get_or_try_init()?.clone()),
                        None => None,
                    },
                    ..self.options.clone()
                },
            )
        })
    }

    /// Do a GET request without following redirects.
    pub fn get_no_redirect<U: reqwest::IntoUrl>(
        &self,
        url: U,
    ) -> Result<reqwest::RequestBuilder, AnyError> {
        Ok(self.client()?.get(url))
    }
}
