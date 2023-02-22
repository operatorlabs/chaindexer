use crate::storage::StorageConf;
pub use crate::storage::DEFAULT_DATADIR;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use toml::Value;

/// config is always in data dir for simplicity
pub const DEFAULT_CONF_FILE: &str = "config.toml";
/// Global conf is a TOML file. Sections of the TOML file are referenced in other args
/// so that all the various aspects of the configuration can be found in a single place.
#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalConf {
    /// location to store app data. app data is stuff like:
    /// - query history
    /// - data maps that use the local variant for their persistence layer.
    pub app_data_dir: std::path::PathBuf,
    /// chain configs are dynamic so they are untyped toml values.
    #[serde(default)]
    pub chains: HashMap<String, Value>,
    /// stores config can be typed
    #[serde(default)]
    pub stores: HashMap<String, StorageConf>,
}

pub const DEFAULT_INDEX_NAME: &str = "eth_mapping.db";
impl Default for GlobalConf {
    fn default() -> Self {
        let eth_basic: Value = toml::from_str(
            // chain confs are currently untyped (until they are used to instantiate chains)
            r#"rpc = {url = "http://localhost:8545", batch_size = 100 }"#,
        )
        .expect("hard coded toml literal should parse");
        let app_data_dir = DEFAULT_DATADIR.to_path_buf();
        let mut chains = HashMap::new();
        let mut stores = HashMap::new();
        chains.insert("eth".to_owned(), eth_basic);
        stores.insert(
            "local_default".to_owned(),
            StorageConf::File {
                dirpath: DEFAULT_DATADIR.to_path_buf(),
                filename: DEFAULT_INDEX_NAME.into(),
            },
        );
        Self {
            app_data_dir,
            chains,
            stores,
        }
    }
}

pub fn default_example_conf() -> String {
    let mut lines: Vec<String> = vec![];
    lines.push(
        "# app data stored here, also default location for locally stored chain data".to_string(),
    );
    lines.push(format!(
        "app_data_dir=\"{}\"",
        DEFAULT_DATADIR.to_str().unwrap().to_owned()
    ));
    lines.push("############# configs for building chain data mappings #############".to_string());
    lines.push("# chain configs for interacting w/ chain...".to_string());
    lines.push("# chain configs are needed when building data maps".to_string());
    lines.push("[chains.eth]".to_string());
    lines.push(r#"rpc = { url="http://localhost:8545", batch_size = 100 }"#.to_string());
    lines.push(
        "# stores are used to define a persistence layer for chain data maps you build".to_string(),
    );
    lines.push("# every data map you build has its own store".to_string());
    lines.push("[stores.eth]".to_string());
    lines.push("type=\"file\"".to_owned());
    lines.push(format!(
        "dirpath=\"{}\"",
        DEFAULT_DATADIR.to_str().unwrap().to_owned()
    ));
    lines.push(format!("filename=\"{DEFAULT_INDEX_NAME}\""));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conf() {
        let _config: GlobalConf = toml::from_str(
            r#"
            app_data_dir = "./testy"
            [chains]
            [chains.eth]
            rpc_url = "http://localhost:8545"
            rpc_batch_size = 100
            [stores.local1]
            type = "file"
            filename="testy.json"
            "#,
        )
        .unwrap();
    }

    #[test]
    fn test_example_default() {
        let rawtoml = default_example_conf();
        println!("{rawtoml}");
        let parsed: GlobalConf =
            toml::from_str(&rawtoml).expect("example default is invalid conf!!");
        assert_eq!(parsed.app_data_dir, DEFAULT_DATADIR.to_path_buf());
    }
}
