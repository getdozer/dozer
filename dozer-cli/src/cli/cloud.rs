use clap::{Args, Subcommand};

use dozer_types::constants::DEFAULT_CLOUD_TARGET_URL;

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Cloud {
    #[arg(
    global = true,
    short = 't',
    long,
    default_value = DEFAULT_CLOUD_TARGET_URL
    )]
    pub target_url: String,
    #[command(subcommand)]
    pub command: CloudCommands,
}

#[derive(Debug, Subcommand, Clone)]
pub enum CloudCommands {
    /// Deploy application to Dozer Cloud
    Deploy(DeployCommandArgs),
    /// Update existing application on Dozer Cloud
    Update(UpdateCommandArgs),
    Delete(AppCommand),
    List(ListCommandArgs),
    Status(AppCommand),
    Monitor(AppCommand),
    Logs(AppCommand),
    /// Application version management
    #[command(subcommand)]
    Version(VersionCommand),
}

#[derive(Debug, Args, Clone)]
pub struct DeployCommandArgs {
    /// Number of replicas to serve Dozer APIs
    #[arg(short, long)]
    pub num_replicas: Option<i32>,
}

pub fn default_num_replicas() -> i32 {
    2
}

#[derive(Debug, Args, Clone)]
pub struct UpdateCommandArgs {
    /// The id of the application to update
    #[arg(short, long)]
    pub app_id: String,
    /// Number of replicas to serve Dozer APIs
    #[arg(short, long)]
    pub num_replicas: Option<i32>,
}

#[derive(Debug, Args, Clone)]
pub struct AppCommand {
    #[arg(short = 'a', long)]
    pub app_id: String,
}

#[derive(Debug, Args, Clone)]
pub struct ListCommandArgs {
    #[arg(short = 'o', long)]
    pub offset: Option<u32>,
    #[arg(short = 'l', long)]
    pub limit: Option<u32>,
    #[arg(short = 'n', long)]
    pub name: Option<String>,
    #[arg(short = 'u', long)]
    pub uuid: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum VersionCommand {
    /// Inspects the status of a version, compared to the current version if it's not current.
    Status {
        /// The version to inspect
        version: u32,
        /// The application id.
        #[clap(short, long)]
        app_id: String,
    },
    /// Creates a new version of the application with the given deployment
    Create {
        /// The deployment of the application to create a new version from
        deployment: u32,
        /// The application id.
        #[clap(short, long)]
        app_id: String,
    },
    /// Sets a version as the "current" version of the application
    ///
    /// Current version of an application can be visited without the "/v<version>" prefix.
    SetCurrent {
        /// The version to set as current
        version: u32,
        /// The application id.
        #[clap(short, long)]
        app_id: String,
    },
}
