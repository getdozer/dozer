use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    // Define the path to the file we want to create or overwrite
    let out_path = Path::new("../json_schemas").join("connections.json");

    // Open the file in write-only mode (this will create the file if it doesn't exist)
    let mut file = File::create(&out_path).expect("Failed to create connections.json");

    let schemas = dozer_types::models::get_connection_schemas().unwrap();
    write!(file, "{}", schemas).expect("Unable to write file");

    // Print a message to indicate the file has been written
    println!("cargo:rerun-if-changed=build.rs");
    println!("Written to {:?}", out_path.display());
}
