//! sql repl interface

use std::sync::Arc;
use std::time::Instant;

use colored::Colorize;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::PrintOptions;
use itertools::Itertools;
use log::{debug, info};

use super::conf::GlobalConf;
use crate::chains::{Chain, ChainApi, ChainConf, ChainDef, EthChain};
use crate::queryeng::ctx::Ctx;
use crate::storage::StorageApi;
use crate::ChainPartitionIndex;
use anyhow::Result;
use datafusion_cli::exec::exec_from_repl;

#[derive(Debug, clap::ValueEnum, Clone)]
enum OutputFormat {
    Table,
    Csv,
    Json,
    JsonL,
}

#[derive(clap::Args, Debug, Clone)]
#[command(version=None, about="\
    This command provides sql interace. By default it spins up a DataFusion REPL"
)]
pub struct SqlCommand {
    /// Format to print the results as. Defaults to a psql style table.
    #[arg(
        value_enum,
        long,
        short,
        value_name = "STRING",
        env = "CHAINDEXER_PRINT_FORMAT",
        default_value = "table"
    )]
    format: OutputFormat,

    /// Execute SQL, print to stdout, and then exit.
    #[arg(long, short)]
    command: Option<String>,
}
impl SqlCommand {
    pub async fn run(&self, global_conf: &GlobalConf) -> Result<()> {
        let mut opts = PrintOptions {
            format: match self.format {
                OutputFormat::Table => PrintFormat::Table,
                OutputFormat::Csv => PrintFormat::Csv,
                OutputFormat::Json => PrintFormat::Json,
                OutputFormat::JsonL => PrintFormat::NdJson,
            },
            quiet: true,
        };
        let ctx = global_conf.init_ctx().await?;
        let mut df_ctx = ctx.ctx_mut();
        if let Some(cmd) = &self.command {
            let now = Instant::now();
            let df = df_ctx.sql(&cmd).await?;
            let results = df.collect().await?;
            opts.print_batches(&results, now)?;
            return Ok(());
        }
        println!("{}", r#"Starting REPL. Close with `\q`"#.blue().bold());
        exec_from_repl(&mut df_ctx, &mut opts).await?;
        Ok(())
    }
}
pub async fn run() {
    let ctx = Ctx::new();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn t() {
        run().await;
    }
}
