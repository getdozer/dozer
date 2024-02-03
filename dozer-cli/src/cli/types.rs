use clap::{Args, Parser, Subcommand};

use super::helper::{DESCRIPTION, LOGO};

use crate::cli::cloud::Cloud;
use dozer_types::{
    constants::{DEFAULT_CONFIG_PATH_PATTERNS, LOCK_FILE},
    serde_json,
};

#[derive(Parser, Debug)]
#[command(author, version, name = "dozer")]
#[command(
    about = format!("{} \n {}", LOGO, DESCRIPTION),
    long_about = None,
)]
pub struct Cli {
    #[arg(
        global = true,
        short = 'c',
        long = "config-path",
        default_values = DEFAULT_CONFIG_PATH_PATTERNS
    )]
    pub config_paths: Vec<String>,
    #[arg(global = true, long, hide = true)]
    pub config_token: Option<String>,
    #[arg(global = true, long = "enable-progress")]
    pub enable_progress: bool,
    #[arg(global = true, long, value_parser(parse_config_override))]
    pub config_overrides: Vec<(String, serde_json::Value)>,

    #[arg(global = true, long = "ignore-pipe")]
    pub ignore_pipe: bool,
    #[clap(subcommand)]
    pub cmd: Commands,
}

fn parse_config_override(
    arg: &str,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut split = arg.split('=');
    let pointer = split.next().ok_or("missing json pointer")?;
    let value = split.next().ok_or("missing json value")?;
    Ok((pointer.to_string(), serde_json::from_str(value)?))
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(
        about = "Initialize an app using a template",
        long_about = "Initialize dozer app workspace. It will generate dozer configuration and \
            folder structure."
    )]
    Init,
    #[command(about = "Edit code interactively")]
    Live(Live),
    #[command(
        about = "Clean home directory",
        long_about = "Clean home directory. It removes all data, schemas and other files in app \
            directory"
    )]
    Clean,
    #[command(
        about = "Initialize and lock schema definitions. Once initialized, schemas cannot \
            be changed"
    )]
    Build(Build),
    #[command(about = "Run App or Api Server")]
    Run(Run),
    #[command(
        about = "Show Sources",
        long_about = "Show available tables schemas in external sources"
    )]
    Connectors(ConnectorCommand),
    #[command(about = "Change security settings")]
    Security(Security),
    #[command(about = "Deploy cloud applications")]
    Cloud(Cloud),
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Live {
    #[arg(long, hide = true)]
    pub disable_live_ui: bool,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Build {
    #[arg(help = format!("Require that {LOCK_FILE} is up-to-date"), long = "locked")]
    pub locked: bool,
    #[arg(short = 'f')]
    pub force: Option<Option<String>>,
}

#[derive(Debug, Args)]
pub struct Run {
    #[arg(help = format!("Require that {LOCK_FILE} is up-to-date"), long = "locked")]
    pub locked: bool,
    #[command(subcommand)]
    pub command: Option<RunCommands>,
}

#[derive(Debug, Subcommand)]
pub enum RunCommands {
    #[command(
        about = "Run app instance",
        long_about = "Run app instance. App instance is responsible for ingesting data and \
            passing it through pipeline"
    )]
    App,
    #[command(
        about = "Run api instance",
        long_about = "Run api instance. Api instance runs server which creates access to \
            API endpoints through REST and GRPC (depends on configuration)"
    )]
    Api,
    #[command(
        about = "Run lambda functions",
        long_about = "Run lambda functions. Lambda functions are JavaScript or Python functions that are called when a new operation is output."
    )]
    Lambda,
    UI
}

#[derive(Debug, Args)]
pub struct Security {
    #[command(subcommand)]
    pub command: SecurityCommands,
}

#[derive(Debug, Subcommand)]
pub enum SecurityCommands {
    #[command(
        author,
        version,
        about = "Generate master token",
        long_about = "Master Token can be used to create other run time tokens \
        that encapsulate different permissions."
    )]
    GenerateToken,
}

#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct Deploy {
    pub target_url: String,
    #[arg(short = 'u')]
    pub username: Option<String>,
    #[arg(short = 'p')]
    pub password: Option<String>,
}

#[derive(Debug, Args)]
pub struct ConnectorCommand {
    #[arg(short = 'f')]
    pub filter: Option<String>,
}

#[cfg(test)]
mod tests {
    use dozer_types::serde_json;

    #[test]
    fn test_parse_config_override_string() {
        let arg = "/app=\"abc\"";
        let result = super::parse_config_override(arg).unwrap();
        assert_eq!(result.0, "/app");
        assert_eq!(result.1, serde_json::Value::String("abc".to_string()));
    }

    #[test]
    fn test_parse_config_override_number() {
        let arg = "/app=123";
        let result = super::parse_config_override(arg).unwrap();
        assert_eq!(result.0, "/app");
        assert_eq!(result.1, serde_json::Value::Number(123.into()));
    }

    #[test]
    fn test_parse_config_override_object() {
        let arg = "/app={\"a\": 1}";
        let result = super::parse_config_override(arg).unwrap();
        assert_eq!(result.0, "/app");
        assert_eq!(
            result.1,
            serde_json::json!({
                "a": 1
            })
        );
    }
}
