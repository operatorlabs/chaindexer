use chaindexer::{entrypoint, CliError};
use colored::Colorize;
use log::{debug, error};

#[tokio::main]
async fn main() {
    env_logger::init();
    match entrypoint(std::env::args_os()).await {
        Ok(_) => {
            debug!("exiting CLI...")
        }
        Err(err) => {
            let errmsg_colored = match err {
                CliError::ConfigError { message } => {
                    format!("{}: {}", "Invalid configuration: ".red().bold(), message)
                }
                CliError::ArgError { arg, message } => {
                    format!(
                        "{} ({})",
                        format!("Bad value for {}", arg.cyan()).red().bold(),
                        message
                    )
                }
                CliError::CommandFailed {
                    command,
                    message,
                    err,
                } => {
                    error!("{err:?}");
                    format!(
                        "{}: \n{}",
                        format!("command {} failed", command.cyan()).red().bold(),
                        message
                    )
                }
            };
            println!("{errmsg_colored}");
            std::process::exit(1);
        }
    }
}
