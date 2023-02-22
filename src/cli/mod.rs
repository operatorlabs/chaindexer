mod build_index;
mod conf;
mod sql;

use self::build_index::BuildChainErr;
use build_index::BuildIndexCommand;
use clap::{Parser, Subcommand};
use colored::Colorize;
use conf::{default_example_conf, GlobalConf, DEFAULT_CONF_FILE, DEFAULT_DATADIR};
use log::{debug, error, info};
use std::{env::ArgsOs, path::PathBuf};
use thiserror::Error;

/// entrypoint for executing the CLI...
pub async fn entrypoint(args: ArgsOs) -> Result<(), CliError> {
    let cli = Cli::parse_from(args);
    let datadir = cli.data_dir.unwrap_or(DEFAULT_DATADIR.to_path_buf());
    cli.command.run(datadir, cli.config).await
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error("Invalid configuration! {message}")]
    ConfigError { message: String },
    #[error("Got invalid value for '{arg}':  {message}")]
    ArgError { arg: String, message: String },
    #[error("Command '{command}' failed:  {message}")]
    CommandFailed {
        command: String,
        message: String,
        err: anyhow::Error,
    },
}
/// Validate and display the current config. Creates a default one if none exists.
#[derive(clap::Args, Debug, Clone)]
#[command(name="config", version=None)]
// TODO: add args and stuff here once we flesh out the config command
pub struct ConfigCommand {}
#[derive(Parser, Debug)]
#[command(version, about = "Indexer and query engine for blockchain data", long_about=None)]
pub struct Cli {
    /// Name of the configuration file.
    #[arg(short, long, value_name = "STRING", env = "CHAINDEXER_CONFIG")]
    /// Directory for application data (including the config file.). Defaults to ~/.chaindexer
    #[arg(short, long, value_name = "PATH", env = "CHAINDEXER_DATA_DIR")]
    data_dir: Option<PathBuf>,
    config: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Index(BuildIndexCommand),
    Config(ConfigCommand),
}

impl Commands {
    pub async fn run(self, datadir: PathBuf, config_file: Option<String>) -> Result<(), CliError> {
        match self {
            Self::Config(_cmd) => {
                let (_conf, confpath, confstr, newone) = load_or_create_conf(config_file, datadir)?;
                let msg = match newone {
                    true => format!("Created new config file at {}:", confpath.display()).green(),
                    false => "Current config:".cyan(),
                };
                print!("{msg} \n\n{confstr} \n");
                Ok(())
            }
            Self::Index(cmd) => {
                let conf = load_conf(config_file, datadir)?;
                // TODO: add confirmation if user might overwrite
                match cmd.run(conf).await {
                    Ok(_) => Ok(()),
                    Err(BuildChainErr::ArgError { arg, message }) => {
                        Err(CliError::ArgError { arg, message })
                    }
                    Err(err) => Err(CliError::CommandFailed {
                        command: "build-index".to_owned(),
                        message: err.to_string(),
                        err: err.into(),
                    }),
                }
            }
        }
    }
}

fn confpath(conf_file: Option<String>, datadir: PathBuf) -> PathBuf {
    let filename = conf_file.unwrap_or(DEFAULT_CONF_FILE.to_owned());
    datadir.join(filename)
}

fn load_conf(config_file: Option<String>, datadir: PathBuf) -> Result<GlobalConf, CliError> {
    let path = confpath(config_file, datadir);
    let confstr = std::fs::read_to_string(path.clone()).map_err(|_| CliError::ConfigError {
        message: format!("failed to open config file at {}", path.display()),
    })?;
    let conf: GlobalConf = toml::from_str(&confstr).map_err(|err| CliError::ConfigError {
        message: err.to_string(),
    })?;
    Ok(conf)
}

fn load_or_create_conf(
    config_file: Option<String>,
    data_dir: PathBuf,
) -> Result<(GlobalConf, PathBuf, String, bool), CliError> {
    let conf_path = confpath(config_file, data_dir.clone());
    let (confstr, confpath, madenew) = match std::fs::read_to_string(conf_path.as_path()) {
        Err(_) => {
            info!(
                "initializing default config file at {}...",
                conf_path.display()
            );
            if !data_dir.as_path().exists() {
                println!(
                    "{}",
                    format!(
                        "data dir {} did not exist... creating one now",
                        data_dir.display()
                    )
                    .cyan()
                );
                std::fs::create_dir(&data_dir).expect("failed to create default datadir");
            }
            let conf = default_example_conf();
            std::fs::write(&conf_path, conf.as_bytes()).expect("failed to write default conf");
            (conf, conf_path, true)
        }
        Ok(s) => (s, conf_path, false),
    };
    let conf: GlobalConf = toml::from_str(&confstr).map_err(|err| CliError::ConfigError {
        message: err.to_string(),
    })?;
    debug!("successfully loaded conf file at {}", confpath.display());
    Ok((conf, confpath, confstr, madenew))
}
