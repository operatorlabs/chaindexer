pub use crate::storage::DEFAULT_DATADIR;
use crate::{
    chains::{Chain, ChainApi, ChainConf, ChainDef, EthChain},
    queryeng::ctx::Ctx,
    storage::{StorageApi, StorageConf},
    ChainPartitionIndex,
};
use colored::Colorize;
use itertools::Itertools;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
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
impl GlobalConf {
    pub async fn init_ctx(&self) -> anyhow::Result<Ctx> {
        let ctx = Ctx::new();
        let mut chains: Vec<Arc<dyn ChainApi>> = Vec::with_capacity(self.chains.len());
        // for every chain they have config'd try to initialize it
        for (id, chain_conf) in &self.chains {
            match Chain::try_from_id_empty(id, Some(chain_conf)) {
                Ok(mut chain) => {
                    assert_eq!(chain.name(), id); // sanity check remove later

                    // look for conf file
                    let mut explicit_conf = None as Option<&str>;
                    if let Some(use_conf) = chain_conf.get("use_conf") {
                        if let Some(s) = use_conf.as_str() {
                            debug!("using config named: {s} for chain {id}");
                            explicit_conf = Some(s);
                        }
                    }

                    let (store_conf, conf_name) = match explicit_conf {
                        Some(n) => (self.stores.get(n), n),
                        None => (self.stores.get(chain.name()), chain.name()),
                    };
                    if let Some(store_conf) = store_conf {
                        info!("found storage conf {store_conf:?}");
                        // if store conf found. try initializing and then loading partition index
                        match StorageApi::<ChainPartitionIndex>::try_new(store_conf.to_owned())
                            .await
                        {
                            Ok(store) => {
                                ctx.add_storage_conf(conf_name, store_conf).await.ok();
                                let maybe_idx = store.load().await?;
                                if let Some(idx) = maybe_idx {
                                    info!("loaded partition index from conf {conf_name}");
                                    chain.set_partition_index(idx);
                                };
                            }
                            Err(err) => {
                                println!(
                                    "{}",
                                    format!(
                                        "failed to initialize storage config {}: {err}",
                                        conf_name.cyan(),
                                    )
                                    .yellow()
                                )
                            }
                        }
                    } else {
                        info!("no storage conf found for {id}");
                    }
                    chains.push(Arc::from(chain));
                }
                Err(_) => {
                    println!(
                        "{}",
                        format!("Failed to initialize chain: {}", id.cyan()).yellow()
                    );
                }
            }
        }
        // we register eth by default so users can run a context without needing
        // to write a config file
        let names = chains.iter().map(|c| c.name()).collect_vec();
        info!("loaded chains {names:?}");
        if !names.contains(&"eth") {
            debug!("no eth chain in users conf file. registering an empty one");
            chains.push(Arc::new(EthChain::new(ChainConf {
                partition_index: None,
                data_fetch_conf: None,
            })));
        }
        // register chains in context
        for c in chains {
            ctx.register_chain(c);
        }
        Ok(ctx)
    }
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
    use crate::{
        chains::test::{ErrorChain, TestChain},
        storage::Location,
        test::TestDir,
    };

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

    #[tokio::test]
    async fn test_init_ctx() {
        let datadir = TestDir::new(true);

        let conf = GlobalConf {
            app_data_dir: datadir.path.to_owned(),
            chains: toml::from_str(&format!(
                "\n[{}] \
                \n# empty conf \
                \n[{}] \
                \n# empty
                ",
                TestChain::ID,
                ErrorChain::ID
            ))
            .unwrap(),
            stores: toml::from_str(&format!(
                "\n[{}] \
                \ntype = \"memory\"\
                \nbucket = \"test\"\
                \n[{}] \
                \ntype = \"memory\" \
                \nbucket = \"test2\" \
                ",
                TestChain::ID,
                ErrorChain::ID
            ))
            .unwrap(),
        };
        let ctx = conf.init_ctx().await.unwrap();
        ctx.catalog().get_chain(TestChain::ID).unwrap();
        ctx.catalog().get_chain(ErrorChain::ID).unwrap();

        ctx.chain_store_for_loc(&Location::new("memory", Some("test"), "/"))
            .await
            .unwrap();
        ctx.chain_store_for_loc(&Location::new("memory", Some("test2"), "/"))
            .await
            .unwrap();
        ctx.chain_store_for_loc(&Location::new("memory", Some("badbucket"), "/"))
            .await
            .unwrap_err();
    }
}
