use clap::{Args, Subcommand};

use clap::ArgAction;
use dozer_services::cloud::Secret;
use std::error::Error;

#[derive(Debug, Args, Clone)]
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
    /// Dozer app secrets management
    #[command(subcommand)]
    Secrets(SecretsCommand),
    /// Get example of API call
    #[command(name = "api-request-samples")]
    ApiRequestSamples {
        #[arg(long, short)]
        endpoint: Option<String>,
    },
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

    #[arg(long = "follow")]
    pub follow: bool,
}

pub fn default_num_api_instances() -> i32 {
    2
}

#[derive(Debug, Args, Clone)]
pub struct LogCommandArgs {
    /// Whether to follow the logs
    #[arg(short, long)]
    pub follow: bool,

    /// The version to inspect
    #[arg(short, long)]
    pub version: Option<u32>,

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
    /// Sets a version as the "current" version of the application
    ///
    /// Current version of an application can be visited without the "/v<version>" prefix.
    SetCurrent {
        /// The version to set as current
        version: u32,
    },
    /// Deletes a version
    ///
    /// This will  delete any resources related to the version, including any
    /// aliases pointing to this version.
    Delete { version: u32 },
    /// Creates or updates an alias to point at the given version
    Alias { alias: String, version: u32 },
    /// Remove alias
    #[command(name = "rm-alias", visible_alias = "rma")]
    RmAlias { alias: String },
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
    if let Some(pos) = s.find('=') {
        Ok(Secret {
            name: s[..pos].to_string(),
            value: s[pos + 1..].to_string(),
        })
    } else {
        let value = std::env::var(s)?;
        Ok(Secret {
            name: s.to_string(),
            value,
        })
    }
}
