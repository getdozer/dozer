use clap::{Args, Subcommand};

use clap::ArgAction;
use dozer_types::grpc_types::cloud::Secret;
use std::error::Error;

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Cloud {
    #[arg(global = true, short = 't', long)]
    pub target_url: Option<String>,

    #[arg(global = true, short, long)]
    pub app_id: Option<String>,

    #[arg(global = true, short, long)]
    pub profile: Option<String>,

    #[command(subcommand)]
    pub command: CloudCommands,
}

#[derive(Debug, Subcommand, Clone)]
pub enum CloudCommands {
    /// Login to Dozer Cloud service
    Login {
        #[arg(long = "organisation_slug")]
        organisation_slug: Option<String>,

        #[arg(global = true, long = "profile_name")]
        profile_name: Option<String>,

        #[arg(global = true, long = "client_id")]
        client_id: Option<String>,

        #[arg(global = true, long = "client_secret")]
        client_secret: Option<String>,
    },
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
        /// App id of application which will be used for all commands
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
    /// List of secrets which will be used in deployment
    #[arg(short, long, value_parser = parse_key_val)]
    pub secrets: Vec<Secret>,

    #[arg(long = "no-lock", action = ArgAction::SetFalse)]
    pub locked: bool,

    #[arg(long = "allow-incompatible")]
    pub allow_incompatible: bool,
}

pub fn default_num_api_instances() -> i32 {
    2
}

#[derive(Debug, Args, Clone)]
pub struct LogCommandArgs {
    /// Whether to follow the logs
    #[arg(short, long)]
    pub follow: bool,

    /// The deployment to inspect
    #[arg(short, long)]
    pub deployment: Option<u32>,

    /// Ignore app logs
    #[arg(long, default_value = "false", action=ArgAction::SetTrue)]
    pub ignore_app: bool,

    /// Ignore api logs
    #[arg(long, default_value = "false", action=ArgAction::SetTrue)]
    pub ignore_api: bool,

    /// Ignore build logs
    #[arg(long, default_value = "false", action=ArgAction::SetTrue)]
    pub ignore_build: bool,
}

#[derive(Debug, Args, Clone)]
pub struct ListCommandArgs {
    /// Offset of the list
    #[arg(short = 'o', long)]
    pub offset: Option<u32>,

    /// Limit of the list
    #[arg(short = 'l', long)]
    pub limit: Option<u32>,

    /// Filter of application name
    #[arg(short = 'n', long)]
    pub name: Option<String>,

    /// Filter of application uuid
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
