//! sql repl interface

use crate::queryeng::ctx::Ctx;
use strum::IntoEnumIterator;
#[derive(clap::Args, Debug, Clone)]
#[command(version=None, about="\
    This command provides sql capabilities. \
    By default this spins up an interactive REPL"
)]
pub struct SqlCommand {}
pub async fn run() {
    let ctx = Ctx::new();
}
