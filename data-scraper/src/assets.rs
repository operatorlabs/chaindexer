use anyhow::{bail, Result};
use log::error;
use log::info;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::DirEntry;
use std::{fs::File, path::PathBuf};

static DIR: Lazy<PathBuf> = Lazy::new(|| {
    PathBuf::new()
        .join(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join("trustwallet")
        .join("assets")
});

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainInfo {
    pub name: String,
    pub website: String,
    pub description: String,
    pub explorer: String,
    pub research: Option<String>,
    pub symbol: String,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
    #[serde(rename = "coin_type")]
    pub coin_type: Option<i64>,
    pub decimals: Option<i64>,
    pub status: Option<String>,
    #[serde(rename = "rpc_url")]
    pub rpc_url: Option<String>,
    pub links: Vec<Link>,
}
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub name: String,
    pub symbol: String,
    pub decimals: i64,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
    pub description: Option<String>,
    pub website: Option<String>,
    pub explorer: Option<String>,
    pub status: Option<String>,
    pub id: Option<String>,
    pub links: Option<Vec<Link>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    pub name: String,
    pub url: String,
}

pub struct Chain {
    pub info: ChainInfo,
    pub assets: HashMap<String, Asset>,
}

pub fn load_chain(chain: &str) -> Result<Chain> {
    let path = DIR.to_path_buf().join("blockchains").join(chain);
    if !path.exists() {
        bail!("path {} does not exist", path.display());
    }
    // load info
    let info_file = File::open(path.join("info").join("info.json"))?;
    let info: ChainInfo = serde_json::from_reader(info_file)?;
    // load assets
    let assets_dir = path.join("assets");
    let mut assets = HashMap::new();
    if assets_dir.exists() {
        for v in std::fs::read_dir(assets_dir)? {
            let entry = v?;
            let res = load_asset(&entry);
            match res {
                Ok((addr, asset)) => {
                    assets.insert(addr, asset);
                }
                Err(err) => {
                    error!(
                        "failed to load asset at path {} due to {err}",
                        entry.path().display()
                    );
                }
            }
            if assets.len() % 100 == 0 {
                info!("loaded {} assets so far", assets.len());
            }
        }
    }
    Ok(Chain { info, assets })
}

fn load_asset(entry: &DirEntry) -> Result<(String, Asset)> {
    let filename = entry.file_name();
    let address = filename.to_str().unwrap();
    let f = File::open(entry.path().join("info.json"))?;
    let asset: Asset = serde_json::from_reader(f)?;
    Ok((address.to_string(), asset))
}

pub fn codegen(chains: Vec<&str>) -> String {
    let mut buf = "use chaindexer_data_scraper::assets::*;\
                  \nuse std::collections::HashMap;"
        .to_string();
    for chain in chains {
        let assets = load_chain(chain).unwrap();
        let mut assets_buf = "".to_string();
        for (addr, v) in assets.assets {
            if !v.name.is_ascii() {
                continue;
            }
            let val = format!(
                "serde_json::from_value(serde_json::json!({})).unwrap()",
                serde_json::to_string(&v).unwrap()
            );
            assets_buf.push_str(&format!(
                "    assets.insert(\"{addr}\".to_string(), {val} );\n"
            ));
        }
        let info = format!(
            "let info: ChainInfo = serde_json::from_value(serde_json::json!({})).unwrap()",
            serde_json::to_string(&assets.info).unwrap()
        );
        buf.push_str(&format!(
            "\npub fn {chain}_assets() -> Chain {{\
                 \n    {info};\
                 \n    let mut assets = HashMap::new();\
                 \n{assets_buf}\
                 \n    Chain {{ info, assets }}\
            \n}}",
        ))
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_chain() {
        load_chain("optimism").unwrap();
    }
}
