use std::process::Command;
use std::{env, path::PathBuf};
#[allow(clippy::all)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .build_client(false)
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .file_descriptor_set_path(out_dir.join("dozer_admin_grpc_descriptor.bin"))
        .compile(&["protos/api.proto"], &["proto"])
        .unwrap();
    // build dozer-orchestrator
    //cargo +nightly build -Z unstable-options --manifest-path=Cargo.toml --release --bin dozer --out-dir dozer-admin/dozer-bin
    let orchestrator_cli = Command::new("cargo")
        .args([
            "+nightly",
            "build",
            "-Z",
            "unstable-options",
            "-p",
            "dozer-orchestrator",
            "--release",
            "--bin",
            "dozer",
            "--out-dir",
            &env::var("OUT_DIR").unwrap(),
        ])
        .status()
        .unwrap();
    if !orchestrator_cli.success() {
        panic!("Cannot build dozer-orchestrator cli");
    }

    // run reset db and run migration
    Command::new("diesel")
        .args(["database", "reset"])
        .status()
        .unwrap();
    Ok(())
}
