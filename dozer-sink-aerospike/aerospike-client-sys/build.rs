use core::panic;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn cp_r(dir: &Path, dest: &Path) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let dst = dest.join(path.file_name().expect("Failed to get filename of path"));
        if fs::metadata(&path).unwrap().is_file() {
            fs::copy(path, dst).unwrap();
        } else {
            fs::create_dir_all(&dst).unwrap();
            cp_r(&path, &dst);
        }
    }
}
fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let build_dir = out_dir.join("build");
    fs::create_dir_all(&build_dir).unwrap();
    let output_dir = build_dir.join("out");
    let lib_dir = output_dir.join("lib");
    let include_dir = output_dir.join("include");
    let make_flags = vec!["TARGET_BASE=out"];

    let current_dir = env::current_dir().unwrap();
    let source_dir = current_dir.join("aerospike-client-c");
    cp_r(&source_dir, &build_dir);

    let mut make = Command::new("make");
    make.args(make_flags)
        .env("MAKEFLAGS", std::env::var("CARGO_MAKEFLAGS").unwrap())
        // The Makefile checks whether DEBUG is defined and cargo always sets it
        // (it's either DEBUG=false or DEBUG=true, but always defined). When DEBUG,
        // it tries to link against gcov, which we don't want
        .env_remove("DEBUG")
        .current_dir(build_dir);
    let out = make.output().unwrap();
    if !out.status.success() {
        panic!(
            "Building aerospike client failed with exit code {}.\nstout: {}\nstderr: {}",
            out.status.code().unwrap(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=static=aerospike");
    println!("cargo:rustc-link-lib=ssl");
    println!("cargo:rustc-link-lib=crypto");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=pthread");

    println!("cargo:rerun-if-changed=aerospike_client.h");
    let bindings = bindgen::Builder::default()
        .header("aerospike_client.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .allowlist_type("(as|aerospike)_.*")
        .allowlist_type("aerospike")
        .allowlist_function("(as|aerospike)_.*")
        .allowlist_var("(as|AS)_.*")
        .clang_arg(format!("-I{}", include_dir.to_str().unwrap()))
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_dir.join("generated.rs"))
        .expect("Failed to write bindings");
}
