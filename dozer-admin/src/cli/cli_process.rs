use std::{path::Path, process::Command};

use super::{utils::init_db, AdminCliConfig};
use crate::server;

pub struct CliProcess {
    pub config: AdminCliConfig,
}
impl CliProcess {
    fn start_ui_server(&self) {
        let ui_path = Path::new(&self.config.ui_path);
        if ui_path.exists() {
            // execute command serve
            Command::new("serve")
                .arg("-s")
                .arg(&self.config.ui_path)
                .spawn()
                .expect("Start ui server failed");
        }
    }
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        init_db();

        // start ui
        self.start_ui_server();

        server::start_admin_server(self.config.to_owned()).await?;
        Ok(())
    }
}
