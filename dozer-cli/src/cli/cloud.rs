use clap::{Args, Subcommand};

use dozer_types::constants::DEFAULT_CLOUD_TARGET_URL;

use dozer_types::grpc_types::cloud::Secret;
use std::error::Error;

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Cloud {
    // Workaround to hide config_path from help in cloud
    #[arg(hide = true)]
    pub config_path: Option<String>,
    #[arg(
    global = true,
    short = 't',
    long,
    default_value = DEFAULT_CLOUD_TARGET_URL
    )]
    pub target_url: String,

    #[arg(global = true, short)]
    pub app_id: Option<String>,

    #[arg(global = true, short)]
    pub profile: Option<String>,

    #[command(subcommand)]
    pub command: CloudCommands,
}

#[derive(Debug, Subcommand, Clone)]
pub enum CloudCommands {
    /// Login to Dozer Cloud service
    Login(OrganisationCommand),
    /// Deploy application to Dozer Cloud
    Deploy(DeployCommandArgs),
    /// Stop and delete application from Dozer Cloud
    Delete,
    /// Get status of running application in Dozer Cloud
    Status,
    /// Monitor processed data amount in Dozer Cloud
    Monitor,
    /// Inspect application logs
    Logs(LogCommandArgs),
    /// Dozer application version management
    #[command(subcommand)]
    Version(VersionCommand),
    /// Set application, which will be used for all commands
    SetApp {
        app_id: String,
    },
    /// List all dozer application in Dozer Cloud
    List(ListCommandArgs),
    /// Dozer API server management
    #[command(subcommand)]
    Api(ApiCommand),
    /// Dozer app secrets management
    #[command(subcommand)]
    Secrets(SecretsCommand),
}

#[derive(Debug, Args, Clone)]
pub struct DeployCommandArgs {
    /// Number of replicas to serve Dozer APIs
    #[arg(short, long)]
    pub num_api_instances: Option<i32>,

    #[arg(short, value_parser = parse_key_val)]
    pub secrets: Vec<Secret>,
}

pub fn default_num_api_instances() -> i32 {
    2
}

#[derive(Debug, Args, Clone)]
pub struct OrganisationCommand {
    #[arg(long = "organisation-name")]
    pub organisation_name: String,
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
    SetNumApiInstances {
        /// The number of replicas to set
        num_api_instances: i32,
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

fn parse_key_val(s: &str) -> Result<Secret, Box<dyn Error + Send + Sync + 'static>> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;

    Ok(Secret {
        name: s[..pos].parse()?,
        value: s[pos + 1..].parse()?,
    })
}
