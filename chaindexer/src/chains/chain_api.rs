use std::sync::Arc;

use crate::{
    partition_index::ChainPartitionIndex,
    table_api::{TableApi, TableApiError},
};
use anyhow::Result;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug)]
pub struct ChainConf<T: Send + Sync + std::fmt::Debug> {
    /// data about all data partitions that can be queried.
    /// this is required if you want to use the chain to query data.
    pub partition_index: Option<ChainPartitionIndex>,
    /// Dynamic, chain/implementation specific configuration for resolving
    /// chain data...
    ///
    /// This could include stuff like RPC url, RPC batchsize etc.
    /// This config object is dynamic b/c no assumptions are made about **how** the data
    /// is resolved.
    ///
    /// This is optional b/c you don't need a data fetching config if you are
    /// just running queries.
    pub data_fetch_conf: Option<T>,
}
impl<T> Default for ChainConf<T>
where
    T: Send + Sync + std::fmt::Debug,
{
    fn default() -> Self {
        Self {
            partition_index: None,
            data_fetch_conf: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ChainApiError {
    #[error("operation not supported: no data fetching config is present")]
    NoDataFetchingConf,
    #[error("error occurred within table")]
    TableError(#[from] TableApiError),
}

#[async_trait]
pub trait ChainApi: Send + Sync + std::fmt::Debug {
    /// Initializes the tables for this chain, returning list of refs to them.
    fn get_tables(self: Arc<Self>) -> Vec<Arc<dyn TableApi>>;
    /// Name of the chain.
    fn name(&self) -> &str;
    fn blocks_per_part(&self) -> u64;
    /// Fetch the most reent blocknum for this chain.
    async fn max_blocknum(&self) -> Result<u64>;
    fn partition_index(&self) -> Option<ChainPartitionIndex>;
    fn set_partition_index(&mut self, data: ChainPartitionIndex);
}

/// Trait for defining a chain and the queryable entities it has.
#[async_trait]
pub trait ChainDef: Sized + Send + Sync {
    /// Runtime config. Might include stuff such as:
    /// - rpc URL
    /// - rpc batch size
    type DynConf: Send + Sync + std::fmt::Debug;
    /// id/name of the chain, e.g. "ethereum"
    const ID: &'static str;
    /// recommended/default blocks per partition
    const BLOCKS_PER_PARTITION: u64;
    /// Initialize chain with a [`ChainConf`] trait obj to enforce injecting the
    /// partition conf as well as the dynamic data fetch.
    fn new(conf: ChainConf<Self::DynConf>) -> Self;
    /// Get a reference to the chains partition config that was passed in during
    /// creation (see this traits `new`)
    fn get_chain_partition_index(&self) -> Option<ChainPartitionIndex>;
    /// Name of the chain. e.g "ethereum"
    fn chain_name(&self) -> &str {
        Self::ID
    }
    /// Default partition size
    fn partsize(&self) -> u64 {
        Self::BLOCKS_PER_PARTITION
    }
    /// Sets the underlying partition index.
    fn set_chain_partition_index(&self, datamap: ChainPartitionIndex);
    /// Initializes a list of references to each table that the chain defines.
    /// Conf data passed into the tables when they are being instantiated here.
    fn tables(self: &Arc<Self>) -> Vec<Arc<dyn TableApi>>;
    /// Fetch the most recent block number for this chain
    async fn newest_block_num(&self) -> Result<u64>;

    /// Using the default ipns location for the partition conf supplied in the source code,
    /// attempt to fetch the config and return an initialized chain.
    async fn try_bootstrap_from_default() -> Result<Self> {
        todo!()
    }
}

#[async_trait]
impl<T> ChainApi for T
where
    T: ChainDef + std::fmt::Debug + Send + Sync,
{
    fn get_tables(self: Arc<Self>) -> Vec<Arc<dyn TableApi>> {
        self.tables()
    }
    fn name(&self) -> &str {
        self.chain_name()
    }
    fn blocks_per_part(&self) -> u64 {
        self.partsize()
    }
    async fn max_blocknum(&self) -> Result<u64> {
        let num = self.newest_block_num().await?;
        Ok(num)
    }
    fn partition_index(&self) -> Option<ChainPartitionIndex> {
        self.get_chain_partition_index().as_ref().cloned()
    }
    fn set_partition_index(&mut self, data: ChainPartitionIndex) {
        self.set_chain_partition_index(data);
    }
}
