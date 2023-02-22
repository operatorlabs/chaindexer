#![warn(
    clippy::all,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]
#![allow(dead_code)]
#![allow(clippy::from_str_radix_10)]
mod cli;
// expose entrypoint and error for main.rs
pub use cli::{entrypoint, CliError};
pub mod chains;
mod partition_index;
pub use partition_index::*;
mod queryeng;
mod storage;
mod subgraph;
mod table_api;
#[cfg(test)]
mod test;
mod util;
