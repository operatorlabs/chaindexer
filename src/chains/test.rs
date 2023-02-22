use std::sync::Arc;

use crate::{
    partition_index::{BlockPartition, ChainPartitionIndex},
    storage::Location,
    table_api::{BlockNumSet, TableApi},
};
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use ethereum_types::H256;
use itertools::Itertools;
use parking_lot::Mutex;

use super::{ChainApi, ChainConf, ChainDef, ColumnDef, ColumnTypeDef, EntityDef};

/// create a [`ChainPartitionIndex`] for unit testing that is valid for the table.
pub async fn chain_empty_idx(partcount: u64) -> ChainPartitionIndex {
    let partsize = TestChain::BLOCKS_PER_PARTITION;
    let chaindata = ChainPartitionIndex::try_new(TestChain::ID, partsize)
        .await
        .unwrap();
    // let table = PartitionedTable::new(TEST_ENTITY_NAME, TEST_BLOCKNUM_COL, 1000);
    chaindata
        .new_table(TestEntity::NAME, TEST_BLOCKNUM_COL)
        .await
        .unwrap();
    for i in 0..partcount {
        chaindata
            .add_partition(BlockPartition {
                table: TestEntity::NAME.to_string(),
                lower_bound: partsize * i,
                row_count: Some(partsize),
                location: empty_url(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    chaindata
}
/// mock partition url
pub fn empty_url() -> Location {
    let loc = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata");
    Location::new(
        "file",
        None,
        object_store::path::Path::parse(loc.to_str().unwrap()).unwrap(),
    )
}
pub struct TestRawData {
    id: u64,
    text: String,
    hash: H256,
}

pub const TEST_BLOCKNUM_COL: &str = "id";
pub const TEST_MAX_BLOCKNUM: u64 = 15_000_000;
#[derive(Debug)]
pub struct TestEntity {
    // data: Arc<PartitionedTable>
    parent: Arc<dyn ChainApi>,
}
#[async_trait]
impl EntityDef for TestEntity {
    type RawData = TestRawData;
    const NAME: &'static str = "testy_entity";
    fn blocknum_partition_col(&self) -> &str {
        TEST_BLOCKNUM_COL
    }
    fn chain(&self) -> Arc<dyn ChainApi> {
        Arc::clone(&self.parent)
    }
    fn columns(&self) -> Vec<ColumnDef<Self::RawData>> {
        vec![
            ColumnDef {
                name: TEST_BLOCKNUM_COL,
                nullable: false,
                transform: ColumnTypeDef::U64 {
                    from_raw: |x| Some(x.id),
                },
            },
            ColumnDef {
                name: "text",
                nullable: false,
                transform: ColumnTypeDef::VarChar {
                    from_raw: |x| Some(&x.text),
                },
            },
            ColumnDef {
                name: "hash",
                nullable: false,
                transform: ColumnTypeDef::FixedBytes {
                    num_bytes: 32,
                    from_raw: |x| Some(x.hash.as_bytes().to_vec()),
                },
            },
            ColumnDef {
                name: "nullvals",
                nullable: true,
                transform: ColumnTypeDef::U64 { from_raw: |_| None },
            },
        ]
    }

    async fn raw_data_with_blocknums(
        &self,
        blocknums: &BlockNumSet,
    ) -> anyhow::Result<Vec<Arc<Self::RawData>>> {
        // generate deterministic test data
        let res = blocknums
            .iter()
            .map(|id| TestRawData {
                id,
                hash: H256::from_low_u64_be(id),
                text: "test".to_string(),
            })
            .map(Arc::new)
            .collect_vec();
        Ok(res)
    }
}
/// chain for unit tests
#[derive(Debug)]
pub struct TestChain {
    data: Mutex<Option<ChainPartitionIndex>>,
}

impl TestChain {
    pub async fn init() -> Self {
        Self {
            data: Mutex::new(Some(
                ChainPartitionIndex::try_new(Self::ID, Self::BLOCKS_PER_PARTITION)
                    .await
                    .unwrap(),
            )),
        }
    }
}

pub fn empty_chain() -> Arc<dyn ChainApi> {
    Arc::new(TestChain {
        data: Mutex::new(None),
    })
}
pub fn empty_chain_table() -> Arc<dyn TableApi> {
    let chain = empty_chain();
    chain.get_tables()[0].clone()
}
pub fn test_table_schema() -> Arc<Schema> {
    let table = empty_chain_table();
    Arc::new(table.schema())
}

#[async_trait]
impl ChainDef for TestChain {
    type DynConf = ();
    const ID: &'static str = "chain_testy";
    const BLOCKS_PER_PARTITION: u64 = 1000;

    fn new(conf: ChainConf<Self::DynConf>) -> Self {
        Self {
            data: Mutex::new(conf.partition_index),
        }
    }
    fn chain_name(&self) -> &str {
        "chain_testy"
    }
    fn get_chain_partition_index(&self) -> Option<ChainPartitionIndex> {
        self.data.lock().as_ref().cloned()
    }
    fn tables(self: &Arc<Self>) -> Vec<Arc<dyn TableApi>> {
        vec![Arc::new(TestEntity {
            parent: Arc::clone(self) as Arc<dyn ChainApi>,
        })]
    }
    /// Fetch the most recent block number for this chain
    async fn newest_block_num(&self) -> anyhow::Result<u64> {
        Ok(TEST_MAX_BLOCKNUM)
    }

    fn set_chain_partition_index(&self, datamap: ChainPartitionIndex) {
        let mut dat = self.data.lock();
        *dat = Some(datamap);
    }
}

#[derive(Debug)]
pub struct ErrorEntity {
    // data: Arc<PartitionedTable>
    parent: Arc<dyn ChainApi>,
}
#[async_trait]
impl EntityDef for ErrorEntity {
    type RawData = TestRawData;
    const NAME: &'static str = "testy_error_entity";
    fn blocknum_partition_col(&self) -> &str {
        TEST_BLOCKNUM_COL
    }
    fn chain(&self) -> Arc<dyn ChainApi> {
        Arc::clone(&self.parent) as Arc<dyn ChainApi>
    }
    fn columns(&self) -> Vec<ColumnDef<Self::RawData>> {
        use crate::chains::ColumnTypeDef;
        vec![ColumnDef {
            name: TEST_BLOCKNUM_COL,
            nullable: false,
            transform: ColumnTypeDef::U64 {
                from_raw: |x| Some(x.id),
            },
        }]
    }
    async fn raw_data_with_blocknums(
        &self,
        _nums: &BlockNumSet,
    ) -> anyhow::Result<Vec<Arc<Self::RawData>>> {
        Err(anyhow::anyhow!("testy error message"))
    }
}

/// like test chain but one of the tables will always error when it tries to fetch its data
#[derive(Debug)]
pub struct ErrorChain {
    data: Mutex<Option<ChainPartitionIndex>>,
}

impl ErrorChain {
    pub async fn init() -> Self {
        Self {
            data: Mutex::new(Some(
                ChainPartitionIndex::try_new(Self::ID, Self::BLOCKS_PER_PARTITION)
                    .await
                    .unwrap(),
            )),
        }
    }
}

#[async_trait]
impl ChainDef for ErrorChain {
    type DynConf = ();
    const ID: &'static str = "chain_testy";
    const BLOCKS_PER_PARTITION: u64 = 1000;

    fn new(conf: ChainConf<Self::DynConf>) -> Self {
        Self {
            data: Mutex::new(conf.partition_index),
        }
    }
    fn chain_name(&self) -> &str {
        "chain_testy"
    }
    fn get_chain_partition_index(&self) -> Option<ChainPartitionIndex> {
        self.data.lock().as_ref().cloned()
    }
    fn tables(self: &Arc<Self>) -> Vec<Arc<dyn TableApi>> {
        vec![
            Arc::new(TestEntity {
                parent: Arc::clone(self) as Arc<dyn ChainApi>,
            }),
            Arc::new(ErrorEntity {
                parent: Arc::clone(self) as Arc<dyn ChainApi>,
            }),
        ]
    }
    /// Fetch the most recent block number for this chain
    async fn newest_block_num(&self) -> anyhow::Result<u64> {
        Ok(TEST_MAX_BLOCKNUM)
    }

    fn set_chain_partition_index(&self, datamap: ChainPartitionIndex) {
        let mut dat = self.data.lock();
        *dat = Some(datamap);
    }
}
