use super::errors::LiveError;
use super::WEB_PORT;
use dozer_types::log::info;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::env;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::path::PathBuf;

pub async fn fetch_latest_init_schema() -> Result<() , {
    let url = "https://dozer-init-template.s3.ap-southeast-1.amazonaws.com/latest";

    let (key, existing_key, key_changed) = get_key_from_url(url).await?;
    let json_file_name = key.as_str() + ".json"; // Assume the JSON file has a .json extension

    if key_changed {
        info!("Downloading latest file: {}", json_file_name);

        let base_url = "https://dozer-init-template.s3.ap-southeast-1.amazonaws.com/";
        let json_url = &(base_url.to_owned() + json_file_name);

        // Delete existing JSON file if present
        if !existing_key.is_empty() {
            delete_file_if_present(existing_key + ".json")?;
        }

        get_json_from_url(json_url, json_file_name).await?;
    }

    Ok(())
}
// This function gets the latest keys from url and compares it with the existing key
// Returns the latest key, existing key and a boolean indicating if the key has changed
async fn get_key_from_url(url: &str) -> Result<(String, String, bool) , {
    let response = reqwest::get(url).await?.error_for_status()?.text().await?;
    let key = response.trim().to_string();

    let file_path = get_directory_path() + "/local-ui/init-keys.txt";
    let mut existing_key = String::new();
    if let Ok(mut file) = std::fs::File::open(&file_path) {
        file.read_to_string(&mut existing_key)?;
    }
    let existing_key = existing_key.trim().to_string();

    let key_changed = existing_key != key;

    if key_changed {
        std::fs::create_dir_all(get_directory_path() + "/local-ui/")?;
        let mut file = std::fs::File::create(&file_path)?;
        file.write_all(key.as_bytes())?;
    }

    Ok((key, existing_key, key_changed))
}


async fn get_json_from_url(url: &str, file_name: &str) -> Result<(), LiveError> {

    let response = reqwest::get(url).await?.error_for_status()?.text().await?;
    let directory_path = get_directory_path();
    let file_path = Path::new(&directory_path).join("local-ui").join(file_name);

    std::fs::create_dir_all(&directory_path)?;
    let mut file = File::create(&file_path)?;
    file.write_all(response.as_bytes())?;

    Ok(())
}



fn delete_file_if_present(file_name: &str) -> Result<(), LiveError> {
    let directory_path = get_directory_path();
    let file_path = Path::new(&directory_path).join("local-ui").join(file_name);
    info!("deleting file {:?}", file_path);
    if file_path.exists() {
        remove_file(file_path)?;
    }
    let contents_path = Path::new(&directory_path).join("local-ui").join("contents");
    if contents_path.exists() {
        remove_dir_all(contents_path)?;
    }
    Ok(())
}



fn get_directory_path() -> String {
    let home_dir = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/{}", home_dir, ".dozer")
}

fn get_build_path() -> PathBuf {
    let directory_path = get_directory_path();
    Path::new(&directory_path)
        .join("local-ui")
        .join("contents")
        .join("build")
}