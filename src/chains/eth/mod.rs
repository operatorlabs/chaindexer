//! Defines all queryable Ethereum tables. Includes specifying table names, columns, and
//! how the tables are populated w/ the RPC api.
pub mod column_defs;
pub mod raw_data;
pub mod rpc_api;
#[cfg(test)]
pub mod test;

use super::ChainApiError;
use crate::chains::{ChainApi, ChainConf, ChainDef, ColumnDef, EntityDef};
use crate::partition_index::ChainPartitionIndex;
use crate::table_api::BlockNumSet;
use crate::table_api::TableApi;
use crate::util::RpcApiConfig;
use anyhow::Result;
use async_trait::async_trait;
use itertools::Itertools;
use parking_lot::RwLock;
use raw_data::{Block, Log};
use rpc_api::{rpc_defaults, RpcApi};
use serde_derive::Deserialize;
use std::marker::PhantomData;
use std::sync::Arc;

const DEFAULT_BLOCKS_PER_PARTITION: u64 = 10_000;
#[derive(Debug, Deserialize, Clone)]
pub struct EthDynConf {
    #[serde(default = "rpc_defaults")]
    pub rpc: RpcApiConfig,
}
impl Default for EthDynConf {
    fn default() -> Self {
        Self {
            rpc: rpc_defaults(),
        }
    }
}
#[derive(Debug)]
pub struct EthChain {
    pub conf: Option<EthDynConf>,
    /// stored partition conf
    partitions: RwLock<Option<ChainPartitionIndex>>,
    rpc: Option<Arc<RpcApi>>,
}

#[async_trait]
impl ChainDef for EthChain {
    type DynConf = EthDynConf;
    const ID: &'static str = "eth";
    const BLOCKS_PER_PARTITION: u64 = DEFAULT_BLOCKS_PER_PARTITION;
    fn new(mut conf: ChainConf<EthDynConf>) -> Self {
        if let Some(dataconf) = conf.data_fetch_conf.as_mut() {
            if dataconf.rpc.url.is_none() {
                if let Ok(v) = std::env::var("ETH_RPC_API") {
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
            Arc::new(BlocksTable::new(Arc::clone(self))),
            Arc::new(LogsTable::new(Arc::clone(self))),
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
pub struct EthEntity<Raw: Send + Sync> {
    parent: Arc<EthChain>,
    _marker: PhantomData<Raw>,
}

impl<T: Send + Sync> EthEntity<T> {
    pub fn new(parent: Arc<EthChain>) -> Self {
        Self {
            parent,
            _marker: PhantomData,
        }
    }

    pub fn rpc(&self) -> Option<Arc<RpcApi>> {
        self.parent.rpc.as_ref().cloned()
    }
}
pub type BlocksTable = EthEntity<Block>;
pub type LogsTable = EthEntity<Log>;

#[async_trait]
impl EntityDef for EthEntity<Block> {
    type RawData = Block;
    const NAME: &'static str = "blocks";
    fn chain(&self) -> Arc<dyn ChainApi> {
        self.parent.clone()
    }
    fn blocknum_partition_col(&self) -> &str {
        "number"
    }
    fn columns(&self) -> Vec<ColumnDef<Self::RawData>> {
        column_defs::blocks()
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
impl EntityDef for EthEntity<Log> {
    type RawData = Log;
    const NAME: &'static str = "logs";
    fn blocknum_partition_col(&self) -> &str {
        "block_number"
    }
    fn chain(&self) -> Arc<dyn ChainApi> {
        self.parent.clone()
    }
    fn columns(&self) -> Vec<ColumnDef<Self::RawData>> {
        column_defs::logs()
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

#[cfg(test)]
mod tests {
    use super::test::{get_rpc_url, start_block};
    use super::*;
    use crate::test::{integration_test_flag, setup_integration};
    use itertools::Itertools;
    use paste::paste;
    use raw_data::TxnReceipt;
    use test::{data_for_table, EthTable};

    async fn testchain(u: String, batch_size: usize) -> Arc<EthChain> {
        let dynconf = EthDynConf {
            rpc: RpcApiConfig {
                url: Some(u),
                batch_size: Some(batch_size),
                ..Default::default()
            },
        };
        let parts = ChainPartitionIndex::try_new("testy", 50)
            .await
            .expect("failed to initialize chain index data");
        Arc::new(EthChain::new(ChainConf {
            partition_index: Some(parts),
            data_fetch_conf: Some(dynconf),
        }))
    }

    #[test]
    fn test_conf_serde() {
        let v = serde_json::json!({
            "rpc": {
                "url": "asdf",
                "batch_size": 100
            }
        });
        let conf: EthDynConf = serde_json::from_value(v).unwrap();
        assert_eq!(conf.rpc.url.unwrap(), "asdf".to_string());
        let conf_default: EthDynConf = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(conf_default.rpc.url.unwrap(), "http://localhost:8545");
    }

    #[tokio::test]
    async fn test_blocks_to_arrow() {
        let data = data_for_table(EthTable::Blocks);
        let raw: Vec<Arc<Block>> = data
            .response
            .clone()
            .into_iter()
            .map(|r| serde_json::from_value(r.result.unwrap()).unwrap())
            .map(Arc::new)
            .collect();
        let table = BlocksTable::new(testchain("".to_string(), 1).await);
        let numraw = raw.len();
        let arrow = table.raw_to_arrow(raw).unwrap();
        assert_eq!(arrow.num_rows(), numraw);
    }
    #[tokio::test]
    async fn test_logs_to_arrow() {
        let data = data_for_table(EthTable::Logs);
        let raw = data
            .response
            .clone()
            .into_iter()
            .flat_map(|r| serde_json::from_value::<Vec<TxnReceipt>>(r.result.unwrap()).unwrap())
            .collect_vec();
        let logs = raw
            .into_iter()
            .flat_map(|r| r.logs)
            .map(Arc::new)
            .collect_vec();
        let table = LogsTable::new(testchain("".to_string(), 1).await);
        let numraw = logs.len();
        let arrow = table.raw_to_arrow(logs).unwrap();
        assert_eq!(arrow.num_rows(), numraw);
    }
    // =============== integration tests =================
    fn setup_() {
        setup_integration();
        let required_vars = vec!["TEST_ETH_RPC_URL"];
        for v in required_vars {
            if std::env::var(v).is_err() {
                panic!("reuqired environment var {v} not found!");
            }
        }
    }
    /// return early if integration test flag not on. make sure env vars are defined otherwise
    macro_rules! setup {
        () => {
            if integration_test_flag() {
                eprintln!("integration tests are turned on... proceeding with setup");
                setup_();
            } else {
                eprintln!("skipping integration test...");
                // return early
                return;
            }
        };
    }
    async fn get_chain() -> Arc<EthChain> {
        Arc::new(EthChain::new(ChainConf {
            partition_index: Some(ChainPartitionIndex::try_new("ethereum", 50).await.unwrap()),
            data_fetch_conf: Some(EthDynConf {
                rpc: RpcApiConfig {
                    url: Some(get_rpc_url()),
                    batch_size: Some(250),
                    request_timeout_ms: Some(3_000),
                    ..Default::default()
                },
            }),
        }))
    }
    async fn test_table(table: Box<dyn TableApi>, rowcount: u64) {
        let startblock = start_block();
        let batch = table
            .batch_for_blocknums(&BlockNumSet::Range(startblock, startblock + rowcount))
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), rowcount as usize);
    }
    /// macro for integration testing loading `$rowcount` rows for a given entity type
    macro_rules! integration_test_datafetching {
        ($table_type: ident, $rowcount: literal) => {
            paste! {
                #[tokio::test]
                async fn [<integration_test_eth_chain_ $table_type:lower _ $rowcount _rows>]() {
                    setup!();
                    let table = Box::new($table_type::new(get_chain().await));
                    test_table(table, $rowcount).await;
                }
            }
        };
    }
    integration_test_datafetching!(BlocksTable, 100);
    integration_test_datafetching!(BlocksTable, 500);
    integration_test_datafetching!(BlocksTable, 2500);
    integration_test_datafetching!(LogsTable, 100);
    integration_test_datafetching!(LogsTable, 500);
    integration_test_datafetching!(LogsTable, 2500);
}
