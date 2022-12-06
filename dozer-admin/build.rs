use std::path::Path;
use std::process::Command;
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let profile = std::env::var("PROFILE").unwrap();

    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .build_client(false)
        .file_descriptor_set_path(out_dir.join("dozer_admin_grpc_descriptor.bin"))
        .compile(&["protos/api.proto"], &["proto"])
        .unwrap();
    let mut manifest_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    manifest_path.pop();
    manifest_path.push(format!("target/{:}/dozer", profile));
    let dozer_bin_path = manifest_path;
    // check if exist then not have to build again
    if !Path::exists(&dozer_bin_path) {
        let mut orchestrator_cli = Command::new("cargo");
        let params = vec!["build", "-p", "dozer-orchestrator", "--bin", "dozer"];
        orchestrator_cli.args(params);
        // build release if current build profile is release
        if profile == "release" {
            orchestrator_cli.arg("-r");
        }
        let execute_result = orchestrator_cli.status().unwrap();
        if !execute_result.success() {
            panic!("Cannot build dozer-orchestrator cli");
        }
    }
    Command::new("cp")
        .args([dozer_bin_path, out_dir])
        .status()
        .unwrap();
    Ok(())
}
