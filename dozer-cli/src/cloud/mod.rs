use crate::{
    cli::cloud::{Cloud, DeployCommandArgs, ListCommandArgs, LogCommandArgs, SecretsCommand},
    errors::OrchestrationError,
};

mod client;
pub mod cloud_app_context;
mod cloud_helper;
pub mod deployer;
pub mod login;
pub mod monitor;
pub mod progress_printer;
mod token_layer;
pub use client::CloudClient;

pub trait DozerGrpcCloudClient {
    fn deploy(
        &mut self,
        cloud: Cloud,
        deploy: DeployCommandArgs,
        config_paths: Vec<String>,
    ) -> Result<(), OrchestrationError>;
    fn delete(&mut self, cloud: Cloud) -> Result<(), OrchestrationError>;
    fn list(&mut self, cloud: Cloud, list: ListCommandArgs) -> Result<(), OrchestrationError>;
    fn status(&mut self, cloud: Cloud) -> Result<(), OrchestrationError>;
    fn monitor(&mut self, cloud: Cloud) -> Result<(), OrchestrationError>;
    fn trace_logs(&mut self, cloud: Cloud, logs: LogCommandArgs) -> Result<(), OrchestrationError>;
    fn login(
        &mut self,
        cloud: Cloud,
        organisation_slug: Option<String>,
        profile: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Result<(), OrchestrationError>;
    fn execute_secrets_command(
        &mut self,
        cloud: Cloud,
        command: SecretsCommand,
    ) -> Result<(), OrchestrationError>;
}
