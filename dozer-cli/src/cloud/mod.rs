use crate::{
    cli::cloud::{DeployCommandArgs, ListCommandArgs, LogCommandArgs, SecretsCommand},
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
        deploy: DeployCommandArgs,
        config_paths: Vec<String>,
    ) -> Result<(), OrchestrationError>;
    fn create(&mut self, config_paths: Vec<String>) -> Result<(), OrchestrationError>;
    fn delete(&mut self) -> Result<(), OrchestrationError>;
    fn list(&mut self, list: ListCommandArgs) -> Result<(), OrchestrationError>;
    fn status(&mut self) -> Result<(), OrchestrationError>;
    fn monitor(&mut self) -> Result<(), OrchestrationError>;
    fn trace_logs(&mut self, logs: LogCommandArgs) -> Result<(), OrchestrationError>;
    fn login(
        &self,
        organisation_slug: Option<String>,
        profile: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Result<(), OrchestrationError>;
    fn execute_secrets_command(
        &mut self,
        command: SecretsCommand,
    ) -> Result<(), OrchestrationError>;
}
