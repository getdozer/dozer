use std::{path::Path, process::Command, sync::Once};

use dozer_core::dag::errors::ExecutionError;
use dozer_types::log::{debug, error};

fn download(folder_name: &str) {
    let path = std::env::current_dir()
        .unwrap()
        .join(format!("./target/debug/{folder_name}-data"));
    let exists = Path::new(&path).is_dir();
    if !exists {
        let exit_status = Command::new("bash")
            .arg("-C")
            .arg(format!("./scripts/download_{folder_name}.sh"))
            .spawn()
            .expect("sh command failed to start")
            .wait()
            .expect("failed to wait");
        assert!(exit_status.success());
    }
}

static INIT: Once = Once::new();
pub fn init() {
    INIT.call_once(|| {
        dozer_tracing::init_telemetry(false).unwrap();
        download("actor");

        set_panic_hook();
    });
}

fn set_panic_hook() {
    std::panic::set_hook(Box::new(move |panic_info| {
        if let Some(e) = panic_info.payload().downcast_ref::<ExecutionError>() {
            error!("{}", e);
            debug!("{:?}", e);
        // If any errors are sent as strings.
        } else if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            error!("{s:?}");
        } else {
            error!("{}", panic_info);
        }

        std::process::exit(1);
    }));
}
