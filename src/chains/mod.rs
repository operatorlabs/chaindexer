mod chain_api;
mod entity;
pub mod eth;

#[cfg(test)]
pub mod test;
#[cfg(test)]
use test::{ErrorChain, TestChain};

pub use chain_api::*;
pub use entity::{ColumnDef, ColumnTypeDef, EntityDef};
pub use eth::{EthChain, EthDynConf};

use crate::partition_index::ChainPartitionIndex;
use anyhow::{bail, Result};
use toml::Value;

/// All supported chains defined here. If you're adding new chains, make sure you
/// add a new enum variant for it and then implement the match arm in `try_init_empty`.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Chain {
    /// Ethereum
    Eth,
    /// for integration testing the cli
    #[cfg(test)]
    TestChain,
    /// for integration testing the cli
    #[cfg(test)]
    TestErrorChain,
}

impl Chain {
    /// Create a new chain with no backing partition index given the chain id and a conf
    pub fn try_from_id_empty(
        chain_id: &str,
        data_fetching_conf: Option<&Value>,
    ) -> Result<Box<dyn ChainApi>> {
        let chain = match chain_id {
            EthChain::ID => Chain::Eth,
            #[cfg(test)]
            TestChain::ID => Chain::TestChain,
            #[cfg(test)]
            ErrorChain::ID => Chain::TestErrorChain,
            _ => bail!("invalid id: {chain_id}"),
        };
        chain.try_init_empty(data_fetching_conf)
    }

    /// Create a new chain with a partition index. given the chain id and a conf
    pub fn try_from_id(
        chain_id: &str,
        data_fetching_conf: Option<&Value>,
        idx: ChainPartitionIndex,
    ) -> Result<Box<dyn ChainApi>> {
        let mut chain = Self::try_from_id_empty(chain_id, data_fetching_conf)?;
        chain.set_partition_index(idx);
        Ok(chain)
    }

    /// Given [`Chain`] and a config object, initialize a chain with no [`ChainPartitionIndex`].
    pub fn try_init_empty(&self, data_fetching_conf: Option<&Value>) -> Result<Box<dyn ChainApi>> {
        Ok(match self {
            Chain::Eth => {
                // attempt to convert it into eth dynamic conf
                let data_fetching: Option<EthDynConf> = data_fetching_conf
                    .map(|c| c.to_owned().try_into::<EthDynConf>())
                    .map_or(Ok(None), |v| v.map(Some))?;
                Box::new(EthChain::new(ChainConf {
                    partition_index: None,
                    data_fetch_conf: data_fetching,
                }))
            }
            #[cfg(test)]
            Chain::TestChain => Box::new(TestChain::new(ChainConf {
                partition_index: None,
                data_fetch_conf: Some(()),
            })),
            #[cfg(test)]
            Chain::TestErrorChain => Box::new(ErrorChain::new(ChainConf {
                partition_index: None,
                data_fetch_conf: Some(()),
            })),
        })
    }
    /// initialize chain with a [`ChainPartitionIndex`]
    pub fn try_init(
        &self,
        conf: Option<&Value>,
        idx: ChainPartitionIndex,
    ) -> Result<Box<dyn ChainApi>> {
        let mut chain = self.try_init_empty(conf)?;
        chain.set_partition_index(idx);
        Ok(chain)
    }
}
