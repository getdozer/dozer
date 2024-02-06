use actix_files::NamedFile;
use dozer_api::actix_web;
use dozer_api::actix_web::dev::Server;
use dozer_api::actix_web::middleware;
use dozer_api::actix_web::web;
use dozer_api::actix_web::App;
use dozer_api::actix_web::HttpRequest;
use dozer_api::actix_web::HttpServer;
use dozer_types::log::info;
use dozer_types::thiserror;
use dozer_types::thiserror::Error;
use std::env;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use zip::result::ZipError;
use zip::ZipArchive;

#[derive(Error, Debug)]
pub enum DownloaderError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Dozer is not initialized")]
    NotInitialized,
    #[error("Connection {0} not found")]
    ConnectionNotFound(String),
    #[error("Error in initializing live server: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Error in reading or extracting from Zip file: {0}")]
    ZipError(#[from] ZipError),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Cannot start ui server: {0}")]
    CannotStartUiServer(#[source] std::io::Error),
}
async fn fetch_dozer_ui(url: &str, folder_name: &str) -> Result<(), DownloaderError> {
    // let url = "https://dozer-explorer.s3.ap-southeast-1.amazonaws.com/latest";
    let latest_url: &str = &format!("{}/latest", url);
    let (key, existing_key, key_changed) = get_key_from_url(latest_url, folder_name).await?;
    let zip_file_name = key.as_str();
    let prev_zip_file_name = existing_key.as_str();
    if key_changed {
        info!("Downloading latest file: {}", zip_file_name);
        let base_url = &format!("{}/", url);
        let zip_url = &(base_url.to_owned() + zip_file_name);
        if !prev_zip_file_name.is_empty() {
            delete_file_if_present(prev_zip_file_name, folder_name)?;
        }
        get_zip_from_url(zip_url, folder_name, zip_file_name).await?;
    } else {
        info!("Current file is up to date");
    }
    Ok(())
}
pub const LOCAL_APP_UI_DIR: &str = "local-app-ui";
pub async fn fetch_latest_dozer_app_ui_code() -> Result<(), DownloaderError> {
    fetch_dozer_ui(
        "https://dozer-app-ui-local.s3.ap-southeast-1.amazonaws.com",
        LOCAL_APP_UI_DIR,
    )
    .await
}

pub fn validate_if_dozer_app_ui_code_exists() -> bool {
    let directory_path = get_directory_path();
    let file_path = Path::new(&directory_path)
        .join(LOCAL_APP_UI_DIR)
        .join("contents");
    file_path.exists()
}

pub const LIVE_APP_UI_DIR: &str = "live-app-ui";
pub async fn fetch_latest_dozer_explorer_code() -> Result<(), DownloaderError> {
    fetch_dozer_ui(
        "https://dozer-explorer.s3.ap-southeast-1.amazonaws.com",
        LIVE_APP_UI_DIR,
    )
    .await
}

// This function gets the latest keys from url and compares it with the existing key
// Returns the latest key, existing key and a boolean indicating if the key has changed
async fn get_key_from_url(
    url: &str,
    folder_name: &str,
) -> Result<(String, String, bool), DownloaderError> {
    let response = reqwest::get(url).await?.error_for_status()?.text().await?;
    let key = response.trim().to_string();
    let directory_path = format!("{}/{}/", get_directory_path(), folder_name);

    let file_path = format!("{}keys.txt", directory_path); //"/local-ui/keys.txt";
    let mut existing_key = String::new();
    if let Ok(mut file) = std::fs::File::open(&file_path) {
        file.read_to_string(&mut existing_key)?;
    }
    let existing_key = existing_key.trim().to_string();

    let key_changed = existing_key != key;

    if key_changed {
        std::fs::create_dir_all(directory_path)?;
        let mut file = std::fs::File::create(&file_path)?;
        file.write_all(key.as_bytes())?;
    }

    Ok((key, existing_key, key_changed))
}

// This function gets the latest zip from url and extracts the zip file to the local-ui directory
async fn get_zip_from_url(
    url: &str,
    folder_name: &str,
    file_name: &str,
) -> Result<(), DownloaderError> {
    // Download the ZIP file
    let response = reqwest::get(url).await?.error_for_status()?.bytes().await?;
    let mut temp_zip = tempfile::tempfile()?;

    // Save the downloaded ZIP content to a temporary file
    temp_zip.write_all(&response)?;

    // Prepare paths and files for extraction
    let directory_path = get_directory_path();
    let file_path = Path::new(&directory_path).join(folder_name).join(file_name);
    let mut existing_zip = Vec::new();

    // Read existing ZIP content if it exists
    if let Ok(mut file) = File::open(&file_path) {
        file.read_to_end(&mut existing_zip)?;
    }

    // Create necessary directories
    std::fs::create_dir_all(&directory_path)?;
    std::fs::create_dir_all(folder_name)?;
    std::fs::create_dir_all(file_path.parent().unwrap())?;

    // Save the downloaded ZIP content to the final location
    let mut file = File::create(&file_path)?;
    temp_zip.seek(std::io::SeekFrom::Start(0))?;
    std::io::copy(&mut temp_zip, &mut file)?;

    // Extract the ZIP archive
    let archive_file = File::open(&file_path)?;
    let mut archive = ZipArchive::new(archive_file)?;

    let extraction_path = Path::new(&directory_path)
        .join(folder_name)
        .join("contents");
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
fn delete_file_if_present(file_name: &str, folder_name: &str) -> Result<(), std::io::Error> {
    let directory_path = get_directory_path();
    let file_path = Path::new(&directory_path).join(folder_name).join(file_name);
    info!("deleting file {:?}", file_path);
    if file_path.exists() {
        remove_file(file_path)?;
    }
    let contents_path = Path::new(&directory_path)
        .join(folder_name)
        .join("contents");
    if contents_path.exists() {
        remove_dir_all(contents_path)?;
    }
    Ok(())
}

async fn index(_req: HttpRequest, data: web::Data<AppData>) -> actix_web::Result<NamedFile> {
    let folder_name = &data.folder_name;
    let build_path = get_build_path(folder_name);
    let index_path = build_path.join("index.html");
    let path: PathBuf = index_path; // Update this with the actual path to your build folder
    Ok(NamedFile::open(path)?)
}

struct AppData {
    folder_name: String,
}
//This function navigates to the react app and starts it
pub fn start_react_app(port: u16, folder_name: &str) -> Result<Server, std::io::Error> {
    let build_path = get_build_path(folder_name);
    let static_path = build_path.join("static");
    let assets_path = build_path.join("assets");
    let my_folder_name = folder_name.to_owned();
    let server = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .service(actix_files::Files::new("/static", &static_path).show_files_listing())
            .service(actix_files::Files::new("/assets", &assets_path).show_files_listing())
            .app_data(web::Data::new(AppData {
                folder_name: my_folder_name.to_owned(),
            }))
            .route("/{anyname:.*}", web::get().to(index))
    })
    .bind(("0.0.0.0", port))?
    .run();

    Ok(server)
}

fn get_directory_path() -> String {
    let home_dir = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/{}", home_dir, ".dozer")
}

fn get_build_path(folder_name: &str) -> PathBuf {
    let directory_path = get_directory_path();
    Path::new(&directory_path)
        .join(folder_name)
        .join("contents")
        .join("build")
}
