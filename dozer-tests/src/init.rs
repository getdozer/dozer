use std::{path::Path, process::Command, sync::Once};

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
        dozer_tracing::init_telemetry(None, None);
        download("actor");

        dozer_orchestrator::set_panic_hook();
    });
}
