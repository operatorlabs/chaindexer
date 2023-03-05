use chaindexer_data_scraper::*;
fn main() {
    // Tell Cargo that if the given file changes, to rerun this build script.
    println!("cargo:rerun-if-changed=src/lib.rs");
}
