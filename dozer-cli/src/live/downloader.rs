use super::errors::LiveError;
use std::env;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::path::Path;
use std::process::Command;
use zip::ZipArchive;

// This function gets the latest keys from url and compares it with the existing key
// Returns the latest key, existing key and a boolean indicating if the key has changed
pub fn get_key_from_url(url: &str) -> Result<(String, String, bool), LiveError> {
    let response = ureq::get(url).call()?.into_string()?;
    let key = response.trim().to_string();

    let file_path = get_directory_path() + "/local-ui/keys.txt";
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

// This function gets the latest zip from url and extracts the zip file to the local-ui directory
pub fn get_zip_from_url(url: &str, file_name: &str) -> Result<(), LiveError> {
    // Download the ZIP file
    let response = ureq::get(url).call()?.into_reader();
    let mut temp_zip = tempfile::tempfile()?;

    // Save the downloaded ZIP content to a temporary file
    std::io::copy(&mut std::io::BufReader::new(response), &mut temp_zip)?;

    // Prepare paths and files for extraction
    let directory_path = get_directory_path();
    let file_path = Path::new(&directory_path).join("local-ui").join(file_name);
    let mut existing_zip = Vec::new();

    // Read existing ZIP content if it exists
    if let Ok(mut file) = File::open(&file_path) {
        file.read_to_end(&mut existing_zip)?;
    }

    // Create necessary directories
    std::fs::create_dir_all(&directory_path)?;
    std::fs::create_dir_all(&file_path.parent().unwrap())?;

    // Save the downloaded ZIP content to the final location
    let mut file = File::create(&file_path)?;
    temp_zip.seek(std::io::SeekFrom::Start(0))?;
    std::io::copy(&mut temp_zip, &mut file)?;

    // Extract the ZIP archive
    let archive_file = File::open(&file_path)?;
    let mut archive = ZipArchive::new(archive_file)?;

    let extraction_path = Path::new(&directory_path).join("local-ui").join("contents");
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = extraction_path.join(file.mangled_name());

        if (file.name()).ends_with('/') {
            std::fs::create_dir_all(&outpath)?;
        } else if let Some(p) = outpath.parent() {
            if !p.exists() {
                std::fs::create_dir_all(p)?;
            }
            let mut outfile = File::create(&outpath)?;
            std::io::copy(&mut file, &mut outfile)?;
        }
    }

    Ok(())
}

// This function deletes the zip files and the contents directory if key has changed
pub fn delete_file_if_present(file_name: &str) -> Result<(), LiveError> {
    let directory_path = get_directory_path();
    let file_path = Path::new(&directory_path).join("local-ui").join(file_name);
    println!("deleting file {:?}", file_path);
    if file_path.exists() {
        fs::remove_file(file_path)?;
    }
    let contents_path = Path::new(&directory_path).join("local-ui").join("contents");
    if contents_path.exists() {
        fs::remove_dir_all(contents_path)?;
    }
    Ok(())
}

//This function navigates to the react app and starts it
pub fn start_react_app() -> Result<(), LiveError> {
    let directory_path = get_directory_path();
    let build_path = Path::new(&directory_path)
        .join("local-ui")
        .join("contents")
        .join("build");
    Command::new("sh")
        .arg("-c")
        .arg(format!("cd {} && serve -s", build_path.display()))
        .output()?;

    Ok(())
}

pub fn get_directory_path() -> String {
    let home_dir = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/{}", home_dir, ".dozer")
}
