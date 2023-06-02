use std::collections::HashMap;

use dozer_api::rest::DOZER_SERVER_NAME_HEADER;
use dozer_cache::Phase;
use dozer_types::prettytable::{row, table};

use crate::errors::CloudError;

#[derive(Debug)]
pub struct PathStatus {
    count: usize,
    phase: Phase,
}

#[derive(Debug, Default)]
pub struct ServerStatus {
    paths: HashMap<String, PathStatus>,
}

#[derive(Debug)]
pub struct VersionStatus {
    servers: Vec<ServerStatus>,
}

pub async fn get_version_status(
    endpoint: &str,
    version: u32,
    api_available: i32,
) -> Result<VersionStatus, CloudError> {
    let (clients, paths) = probe_dozer_servers(endpoint, version, api_available).await?;

    let mut servers = vec![];
    for client in clients {
        let mut server_status = ServerStatus::default();
        for path in &paths {
            let count = client
                .post(format!("http://{}/v{}{}/count", endpoint, version, path))
                .send()
                .await?
                .error_for_status()?;
            let count = count.json::<usize>().await?;

            let phase = client
                .post(format!("http://{}/v{}{}/phase", endpoint, version, path))
                .send()
                .await?
                .error_for_status()?;
            let phase = phase.json::<Phase>().await?;

            server_status
                .paths
                .insert(path.clone(), PathStatus { count, phase });
        }
        servers.push(server_status)
    }

    Ok(VersionStatus { servers })
}

/// Probe the servers to get a list of clients with sticky session cookie, and all the paths available.
async fn probe_dozer_servers(
    endpoint: &str,
    version: u32,
    count_hint: i32,
) -> Result<(Vec<reqwest::Client>, Vec<String>), CloudError> {
    let mut clients = HashMap::<Vec<u8>, reqwest::Client>::default();
    let mut paths = vec![];

    // Try to visit the version endpoint many times to cover all the servers.
    for _ in 0..count_hint * 5 {
        let client = reqwest::Client::builder().cookie_store(true).build()?;
        let response = client
            .get(format!("http://{}/v{}/", endpoint, version))
            .send()
            .await?
            .error_for_status()?;
        let server_name = response
            .headers()
            .get(DOZER_SERVER_NAME_HEADER)
            .ok_or(CloudError::MissingResponseHeader)?
            .as_bytes();

        if clients.contains_key(server_name) {
            continue;
        }
        clients.insert(server_name.to_vec(), client);

        if paths.is_empty() {
            paths = response.json::<Vec<String>>().await?;
        }
    }

    Ok((clients.into_values().collect(), paths))
}

pub fn version_status_table(status: &Result<VersionStatus, CloudError>) -> String {
    if let Ok(status) = status {
        let mut table = table!();
        for server in &status.servers {
            let mut server_table = table!();
            for (path, PathStatus { count, phase }) in &server.paths {
                let phase = serde_json::to_string(phase).expect("Should always succeed");
                server_table.add_row(row![path, count, phase]);
            }
            table.add_row(row![server_table]);
        }
        table.to_string()
    } else {
        "Unavailable".to_string()
    }
}

pub fn version_is_up_to_date(
    status: &Result<VersionStatus, CloudError>,
    current_status: &Result<VersionStatus, CloudError>,
) -> bool {
    if let Ok(status) = status {
        if let Ok(current_status) = current_status {
            version_is_up_to_date_impl(status, current_status)
        } else {
            true
        }
    } else {
        false
    }
}

/// We say a version is up to date if any server of this version are up to date with any server of current version.
fn version_is_up_to_date_impl(status: &VersionStatus, current_status: &VersionStatus) -> bool {
    for server in &status.servers {
        for current_server in &current_status.servers {
            if server_is_up_to_date(server, current_server) {
                return true;
            }
        }
    }
    true
}

/// We say a server is up to date if all paths are up to date.
fn server_is_up_to_date(status: &ServerStatus, current_status: &ServerStatus) -> bool {
    for (path, status) in &status.paths {
        if !path_is_up_to_date(status, current_status.paths.get(path)) {
            return false;
        }
    }
    true
}

fn path_is_up_to_date(status: &PathStatus, current_status: Option<&PathStatus>) -> bool {
    if let Some(current_status) = current_status {
        status.phase == Phase::Streaming
            || (current_status.phase == Phase::Snapshotting && status.count >= current_status.count)
    } else {
        true
    }
}
