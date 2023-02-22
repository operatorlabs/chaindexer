//! sql repl interface
use super::conf::GlobalConf;

use anyhow::Result;
use colored::Colorize;
use datafusion_cli::exec::exec_from_repl;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::PrintOptions;
use std::time::Instant;

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
    /// Specify this to make the query engine only look at the `last_n` blocks.
    /// This can be useful for running queries without needing to pre-index and
    /// without worrying about specifying a block range in your queries everytime.
    #[arg(long, short = 'n', value_name = "INT")]
    last_n_blocks: Option<u64>,
    /// Execute SQL, print to stdout, and then exit.
    #[arg(long, short, value_name = "SQL")]
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
        let mut ctx = global_conf.init_ctx().await?;
        let df_ctx = ctx.ctx_mut();
        if let Some(cmd) = &self.command {
            let now = Instant::now();
            let df = df_ctx.sql(cmd).await?;
            let results = df.collect().await?;
            opts.print_batches(&results, now)?;
            return Ok(());
        }
        println!("{}", r#"Starting REPL. Close with `\q`"#.blue().bold());
        exec_from_repl(df_ctx, &mut opts).await?;
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn t() {
//         run().await;
//     }
// }
