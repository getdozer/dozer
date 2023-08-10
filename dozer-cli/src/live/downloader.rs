use std::io::Read;
use std::fs;
use std::env;
use std::fs::File;
use std::path::Path;
use std::io::Write;
use zip::ZipArchive;
use super::errors::LiveError;
use std::process::Command;


pub fn get_key_from_url(url: &str) -> Result<(String, String, bool), LiveError> {
    let response = reqwest::blocking::get(url)?.text()?;
    let key = response.to_string().trim().to_string();
    
    let file_path = get_directory_path()+"/local-ui/keys.txt";
    let mut existing_key = String::new();
    if let Ok(mut file) = std::fs::File::open(&file_path) {
        file.read_to_string(&mut existing_key)?;
    }
    let existing_key = existing_key.trim().to_string();
    
    let key_changed = existing_key != key;

    if key_changed {
        std::fs::create_dir_all(get_directory_path()+"/local-ui/")?;
        let mut file = std::fs::File::create(&file_path)?;
        file.write_all(key.as_bytes())?;
    }

    Ok((key,existing_key, key_changed))
}

pub fn get_zip_from_url(url: &str, file_name: &str) -> Result<(), LiveError> {
    let response = reqwest::blocking::get(url)?.bytes()?;
    let file_path = Path::new((get_directory_path()+"/local-ui").as_str()).join(file_name);
    let mut existing_zip = Vec::new();
    if let Ok(mut file) = File::open(&file_path) {
        file.read_to_end(&mut existing_zip)?;
    }
    let directory_path = get_directory_path();
    std::fs::create_dir_all(get_directory_path()+"/local-ui")?;
    let mut file = File::create(&file_path)?;
    file.write_all(response.as_ref())?;

    let path = Path::new(&directory_path).join("local-ui").join("");
    let file = File::open(&file_path)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = path.join(file.mangled_name());
        if (&*file.name()).ends_with('/') {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    std::fs::create_dir_all(&p)?;
                }
            }
            let mut outfile = File::create(&outpath)?;
            std::io::copy(&mut file, &mut outfile)?;
        }
    }

    Ok(())
}

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

pub fn start_react_app() -> Result<(), LiveError> {
    let directory_path = get_directory_path();
    let build_path = Path::new(&directory_path).join("local-ui").join("contents").join("build");
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
