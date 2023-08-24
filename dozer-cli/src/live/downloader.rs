use super::errors::LiveError;
use super::WEB_PORT;
use actix_files::NamedFile;
use dozer_api::actix_web;
use dozer_api::actix_web::dev::Server;
use dozer_api::actix_web::middleware;
use dozer_api::actix_web::web;
use dozer_api::actix_web::App;
use dozer_api::actix_web::HttpRequest;
use dozer_api::actix_web::HttpServer;
use dozer_types::log::info;
use std::env;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use zip::ZipArchive;

pub async fn fetch_latest_dozer_explorer_code() -> Result<(), LiveError> {
    let url = "https://dozer-explorer.s3.ap-southeast-1.amazonaws.com/latest";

    let (key, existing_key, key_changed) = get_key_from_url(url).await?;
    let zip_file_name = key.as_str();
    let prev_zip_file_name = existing_key.as_str();
    if key_changed {
        info!("Downloading latest file: {}", zip_file_name);

        let base_url = "https://dozer-explorer.s3.ap-southeast-1.amazonaws.com/";
        let zip_url = &(base_url.to_owned() + zip_file_name);
        if !prev_zip_file_name.is_empty() {
            delete_file_if_present(prev_zip_file_name)?;
        }
        get_zip_from_url(zip_url, zip_file_name).await?;
    }

    Ok(())
}

// This function gets the latest keys from url and compares it with the existing key
// Returns the latest key, existing key and a boolean indicating if the key has changed
async fn get_key_from_url(url: &str) -> Result<(String, String, bool), LiveError> {
    let response = reqwest::get(url).await?.error_for_status()?.text().await?;
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
async fn get_zip_from_url(url: &str, file_name: &str) -> Result<(), LiveError> {
    // Download the ZIP file
    let response = reqwest::get(url).await?.error_for_status()?.bytes().await?;
    let mut temp_zip = tempfile::tempfile()?;

    // Save the downloaded ZIP content to a temporary file
    temp_zip.write_all(&response)?;

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
    std::fs::create_dir_all(file_path.parent().unwrap())?;

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

async fn index(_req: HttpRequest) -> actix_web::Result<NamedFile> {
    let build_path = get_build_path();
    let index_path = build_path.join("index.html");
    let path: PathBuf = index_path; // Update this with the actual path to your build folder
    Ok(NamedFile::open(path)?)
}
//This function navigates to the react app and starts it
pub fn start_react_app() -> Result<Server, std::io::Error> {
    let build_path = get_build_path();
    let static_path = build_path.join("static");
    let assets_path = build_path.join("assets");
    let server = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .service(actix_files::Files::new("/static", &static_path).show_files_listing())
            .service(actix_files::Files::new("/assets", &assets_path).show_files_listing())
            .route("/{anyname:.*}", web::get().to(index))
    })
    .bind(("0.0.0.0", WEB_PORT))?
    .run();
    Ok(server)
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
