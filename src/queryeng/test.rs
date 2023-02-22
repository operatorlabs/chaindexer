use crate::{
    chains::{test::TestChain, ChainApi, ChainConf, ChainDef},
    partition_index::{ChainPartitionIndex, CreatePartitionOpts},
    storage::{StorageApi, StorageConf},
    test::TestDir,
};
use anyhow::Result;
use std::sync::Arc;

/// dto for a chain to unit test with
pub struct ChainTest {
    pub store: Arc<StorageApi<ChainPartitionIndex>>,
    pub chain: Arc<dyn ChainApi>,
    pub datadir: Option<TestDir>,
}
pub enum TestStore {
    Mem,
    File,
    Ipfs,
}
/// options for building test chain
pub struct TestChainOpts {
    pub blocks_per_partition: u64,
    pub end_block: u64,
    pub blocks_per_batch: u64,
    pub batches_per_rowgroup: usize,
    pub store: TestStore,
}
impl Default for TestChainOpts {
    fn default() -> Self {
        Self {
            blocks_per_partition: 1000,
            end_block: 100_000,
            blocks_per_batch: 100,
            batches_per_rowgroup: 1,
            store: TestStore::Mem,
        }
    }
}
/// Create an index for [`TestChain`] and return an instance of the chain with
/// the created index backing it.
pub async fn build_test_chain_with_index(opts: TestChainOpts) -> Result<ChainTest> {
    let empty_chain = Arc::new(TestChain::new(ChainConf {
        partition_index: None,
        data_fetch_conf: Some(()),
        ..Default::default()
    }));
    let tables = empty_chain.clone().get_tables();
    let TestChainOpts {
        blocks_per_partition,
        end_block,
        blocks_per_batch,
        batches_per_rowgroup,
        store,
    } = opts;
    let (store, datadir) = match store {
        TestStore::Mem => (
            StorageApi::try_new(StorageConf::Memory {
                bucket: "testy".to_owned(),
            })
            .await
            .expect("should always be able to initialize an in memory store!"),
            None,
        ),
        TestStore::File => {
            let datadir = TestDir::new(true);
            let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
                dirpath: datadir.path.to_owned(),
                filename: "testy.db".to_string(),
            })
            .await
            .unwrap();
            (store, Some(datadir))
        }
        TestStore::Ipfs => todo!(),
    };
    let index = ChainPartitionIndex::try_new(TestChain::ID, blocks_per_partition).await?;
    for t in tables.iter().cloned() {
        index.register_table(t).await.unwrap();
    }
    for cur_block in (0..end_block).step_by(blocks_per_partition as usize) {
        for table in tables.iter().cloned() {
            index
                .create_partition(
                    cur_block,
                    table,
                    &store,
                    CreatePartitionOpts {
                        blocks_per_batch: Some(blocks_per_batch),
                        batches_per_row_group: Some(batches_per_rowgroup),
                        end: None,
                    },
                )
                .await
                .unwrap();
        }
    }
    Ok(ChainTest {
        store: Arc::new(store),
        datadir,
        chain: Arc::new(TestChain::new(ChainConf {
            partition_index: Some(index),
            data_fetch_conf: Some(()),
            ..Default::default()
        })),
    })
}
