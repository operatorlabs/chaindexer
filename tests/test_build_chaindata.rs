#![allow(clippy::all, dead_code, unused)]
mod shared;
use chaindexer::{entrypoint, CliError};

#[tokio::test]
async fn base_test() {
    let cmd = format!("build-data-map --chain eth --store local_default");
}
