use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;

use super::{
    eth::{
        raw_data::{Block, Log},
        rpc_api::RpcApi,
    },
    ChainApi, ChainApiError, ChainConf, ChainDef, ColumnDef, EntityDef, EthDynConf,
};
use crate::{
    table_api::{BlockNumSet, TableApi},
    ChainPartitionIndex,
};
use anyhow::Result;

#[derive(Debug)]
pub struct PolygonChain {
    pub conf: Option<EthDynConf>,
    /// stored partition conf
    partitions: RwLock<Option<ChainPartitionIndex>>,
    rpc: Option<Arc<RpcApi>>,
}

#[async_trait]
impl ChainDef for PolygonChain {
    type DynConf = EthDynConf;
    const ID: &'static str = "polygon";
    const BLOCKS_PER_PARTITION: u64 = 100_000;
    fn new(mut conf: ChainConf<EthDynConf>) -> Self {
        if let Some(dataconf) = conf.data_fetch_conf.as_mut() {
            if dataconf.rpc.url.is_none() {
                if let Ok(v) = std::env::var("POLYGON_RPC_API") {
                    dataconf.rpc.url = Some(v);
                }
            }
        }
        let rpc = conf
            .data_fetch_conf
            .as_ref()
            .map(|c| Arc::new(RpcApi::new(Self::ID, &c.rpc)));
        Self {
            conf: conf.data_fetch_conf,
            partitions: RwLock::new(conf.partition_index.as_ref().cloned()),
            rpc,
        }
    }
    fn set_chain_partition_index(&self, datamap: ChainPartitionIndex) {
        let mut p = self.partitions.write();
        *p = Some(datamap);
    }
    fn get_chain_partition_index(&self) -> Option<ChainPartitionIndex> {
        self.partitions.read().as_ref().cloned()
    }
    fn tables(self: &Arc<Self>) -> Vec<Arc<dyn TableApi>> {
        // share rpc api to manage rate limiting and concurrency
        vec![
            Arc::new(Entity::<Block>::new(Arc::clone(self))),
            Arc::new(Entity::<Log>::new(Arc::clone(self))),
        ]
    }
    async fn newest_block_num(&self) -> Result<u64> {
        self.rpc
            .as_ref()
            .ok_or(ChainApiError::NoDataFetchingConf)?
            .block_number()
            .await
    }
}

#[derive(Debug)]
struct Entity<Raw: Send + Sync> {
    parent: Arc<PolygonChain>,
    _marker: PhantomData<Raw>,
}

impl<T: Send + Sync> Entity<T> {
    pub fn new(parent: Arc<PolygonChain>) -> Self {
        Self {
            parent,
            _marker: PhantomData,
        }
    }

    pub fn rpc(&self) -> Option<Arc<RpcApi>> {
        self.parent.rpc.as_ref().cloned()
    }
}
#[async_trait]
impl EntityDef for Entity<Block> {
    type RawData = Block;
    const NAME: &'static str = "blocks";
    fn chain(&self) -> Arc<dyn ChainApi> {
        self.parent.clone()
    }
    fn blocknum_partition_col(&self) -> &str {
        "number"
    }
    fn columns(&self) -> Vec<ColumnDef<Self::RawData>> {
        super::eth::column_defs::blocks()
    }
    async fn raw_data_with_blocknums(
        &self,
        nums: &BlockNumSet,
    ) -> Result<Vec<Arc<Self::RawData>>, anyhow::Error> {
        self.rpc()
            .ok_or(ChainApiError::NoDataFetchingConf)?
            .blocks_with_nums(nums)
            .await
            .map(|v| v.into_iter().map(Arc::new).collect())
    }
}

#[async_trait]
impl EntityDef for Entity<Log> {
    type RawData = Log;
    const NAME: &'static str = "logs";
    fn blocknum_partition_col(&self) -> &str {
        "block_number"
    }
    fn chain(&self) -> Arc<dyn ChainApi> {
        self.parent.clone()
    }
    fn columns(&self) -> Vec<ColumnDef<Self::RawData>> {
        super::eth::column_defs::logs()
    }
    async fn raw_data_with_blocknums(
        &self,
        blocknums: &BlockNumSet,
    ) -> Result<Vec<Arc<Self::RawData>>> {
        let recs = self
            .rpc()
            .ok_or(ChainApiError::NoDataFetchingConf)?
            .tx_receipts_for_blocknums(blocknums)
            .await?;
        Ok(recs
            .into_iter()
            .flat_map(|r| r.logs)
            .map(Arc::new)
            .collect_vec())
    }
}
