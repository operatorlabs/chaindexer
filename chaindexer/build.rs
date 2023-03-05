use chaindexer_data_scraper::assets::codegen;
use std::fs::{write, File};
use std::io::Write;
use std::path::PathBuf;

fn assets_gen() {
    // chains to load assets for
    let chains = vec!["ethereum", "polygon"];
    // Tell Cargo that if the given file changes, to rerun this build script.
    let code = codegen(chains);
    let path = PathBuf::new()
        .join(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("generated")
        .join("assets.rs");
    write(path, code.as_bytes()).unwrap();
}

fn main() {
    // println!("cargo:rerun-if-changed=src/lib.rs");
    assets_gen();
}
