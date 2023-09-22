use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let schema_path = Path::new("../json_schemas");
    // Define the path to the file we want to create or overwrite
    let connection_path = schema_path.join("connections.json");
    let dozer_path = schema_path.join("dozer.json");

    let mut file = File::create(connection_path).expect("Failed to create connections.json");
    let schemas = dozer_types::models::get_connection_schemas().unwrap();
    write!(file, "{}", schemas).expect("Unable to write file");

    let mut dozer_schema_file = File::create(dozer_path).expect("Failed to create dozer.json");
    let schema = dozer_types::models::get_dozer_schema().unwrap();
    write!(dozer_schema_file, "{}", schema).expect("Unable to write file");

    // Print a message to indicate the file has been written
    println!("cargo:rerun-if-changed=build.rs");
    println!("Written to {:?}", schema_path.display());
}
