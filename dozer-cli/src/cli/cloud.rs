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
    #[arg(global = true, long)]
    pub profile: Option<String>,
    #[command(subcommand)]
    pub command: CloudCommands,
}

#[derive(Debug, Subcommand, Clone)]
pub enum CloudCommands {
    /// Deploy application to Dozer Cloud
    Deploy(DeployCommandArgs),
    /// Update existing application on Dozer Cloud
    Update(UpdateCommandArgs),
    Delete,
    List(ListCommandArgs),
    Status,
    Monitor,
    /// Inspect application logs
    Logs(LogCommandArgs),
    Login(CompanyCommand),
    /// Application version management
    #[command(subcommand)]
    Version(VersionCommand),
    /// Dozer API server management
    #[command(subcommand)]
    Api(ApiCommand),
    /// Dozer app secrets management
    #[command(subcommand)]
    Secrets(SecretsCommand),
    /// Dozer app context management
    #[command(subcommand)]
    App(AppCommand),
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
    /// Number of replicas to serve Dozer APIs
    #[arg(short, long)]
    pub num_replicas: Option<i32>,
}
#[derive(Debug, Args, Clone)]
pub struct CompanyCommand {
    #[arg(long = "company-name")]
    pub company_name: String,
}

#[derive(Debug, Subcommand, Clone)]
pub enum AppCommand {
    Use { app_id: String },
}

#[derive(Debug, Args, Clone)]
pub struct LogCommandArgs {
    /// Whether to follow the logs
    #[arg(short, long)]
    pub follow: bool,
    /// The deployment to inspect
    #[arg(short, long)]
    pub deployment: Option<u32>,
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
    },
    /// Creates a new version of the application with the given deployment
    Create {
        /// The deployment of the application to create a new version from
        deployment: u32,
    },
    /// Sets a version as the "current" version of the application
    ///
    /// Current version of an application can be visited without the "/v<version>" prefix.
    SetCurrent {
        /// The version to set as current
        version: u32,
    },
}

#[derive(Debug, Clone, Subcommand)]
pub enum ApiCommand {
    /// Sets the number of replicas to serve Dozer APIs
    SetNumReplicas {
        /// The number of replicas to set
        num_replicas: i32,
    },
}

#[derive(Debug, Clone, Subcommand)]
pub enum SecretsCommand {
    /// Creates new secret
    Create {
        /// Name of secret
        name: String,

        /// Value of secret
        value: String,
    },
    /// Update secret value
    Update {
        /// Name of secret
        name: String,

        /// Value of secret
        value: String,
    },
    /// Delete secret
    Delete {
        /// Name of secret
        name: String,
    },
    /// Get secret
    Get {
        /// Name of secret
        name: String,
    },
    /// List all app secrets
    List {},
}
