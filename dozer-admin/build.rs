use std::process::Command;
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let mut manifest_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .build_client(false)
        .file_descriptor_set_path(out_dir.join("dozer_admin_grpc_descriptor.bin"))
        .compile(&["protos/api.proto"], &["proto"])
        .unwrap();
    // build dozer-orchestrator
    let orchestrator_cli = Command::new("cargo")
        .args([
            "build",
            "-p",
            "dozer-orchestrator",
            "--release",
            "--bin",
            "dozer",
        ])
        .status()
        .unwrap();
    if !orchestrator_cli.success() {
        panic!("Cannot build dozer-orchestrator cli");
    }

    //  to go outer path
    manifest_path.pop();
    manifest_path.push("target/release/dozer");
    let dozer_bin_path = manifest_path;
    Command::new("cp")
        .args([dozer_bin_path, out_dir])
        .status()
        .unwrap();
    Ok(())
}
