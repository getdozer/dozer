use std::collections::HashMap;

use dozer_cache::Phase;
use dozer_types::prettytable::{row, table};

#[derive(Debug)]
pub struct EndpointStatus {
    count: usize,
    phase: Phase,
}

pub async fn get_version_status(
    endpoint: &str,
    version: u32,
) -> Result<HashMap<String, EndpointStatus>, reqwest::Error> {
    let client = reqwest::Client::default();

    let response = client
        .get(format!("http://{}/v{}/", endpoint, version))
        .send()
        .await?
        .error_for_status()?;
    let paths = response.json::<Vec<String>>().await?;
    let mut result = HashMap::default();

    for path in paths {
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

        result.insert(path, EndpointStatus { count, phase });
    }

    Ok(result)
}

pub fn version_status_table(
    status: &Result<HashMap<String, EndpointStatus>, reqwest::Error>,
) -> String {
    if let Ok(status) = status {
        let mut table = table!();
        for (path, EndpointStatus { count, phase }) in status {
            let phase = serde_json::to_string(phase).expect("Should always succeed");
            table.add_row(row![path, count, phase]);
        }
        table.to_string()
    } else {
        "Unavailable".to_string()
    }
}

pub fn version_is_up_to_date(
    status: &Result<HashMap<String, EndpointStatus>, reqwest::Error>,
    current_status: &Result<HashMap<String, EndpointStatus>, reqwest::Error>,
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

fn version_is_up_to_date_impl(
    status: &HashMap<String, EndpointStatus>,
    current_status: &HashMap<String, EndpointStatus>,
) -> bool {
    for (path, status) in status {
        if !endpoint_is_up_to_date(status, current_status.get(path)) {
            return false;
        }
    }
    true
}

fn endpoint_is_up_to_date(
    status: &EndpointStatus,
    current_status: Option<&EndpointStatus>,
) -> bool {
    if let Some(current_status) = current_status {
        status.phase == Phase::Streaming
            || (current_status.phase == Phase::Snapshotting && status.count >= current_status.count)
    } else {
        true
    }
}
