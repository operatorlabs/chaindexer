//! Command for building a chain's data map.
use super::conf::GlobalConf;
use crate::{
    chains::{Chain, ChainApi},
    partition_index::{BlockPartition, ChainPartitionIndex, CreatePartitionOpts},
    storage::{StorageApi, StorageConf},
    table_api::TableApi,
};
use anyhow::Result;
use clap::ValueEnum;
use colored::Colorize;
use dialoguer::{theme::ColorfulTheme, Confirm, Input};
use itertools::Itertools;
use log::{debug, info};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

/// This command is used to create a partition index for a given chain. Right now
/// this only supports creating a partition index of a chains raw data.
///
/// A partition index is used by the query engine to know where it needs to fetch data from.
/// For each table in a chain's data map, there are many persisted parquet files where
/// that table's data is stored. Each table's data is partitioned by block number,
/// hence there are many files per table.
///
/*
/// While this command is for building your own data maps, you can also use data maps
/// provided externally. For example, chains may have data maps maintained and stored
/// on IPFS. However, if you want to use the query engine entirely trustlessly,
/// you'll have to build your own data maps with your own nodes, since using data maps
/// provided by others means you are trusting that the underlying data is correct.
///  */
#[derive(clap::Args, Debug, Clone)]
#[command(version=None, about="\
    Build a partition index. \
    Currently this only supports building a chain's partition index. \
    See this command's help for more info."
)]
pub struct BuildIndexCommand {
    /// Id of chain to build index for. Also identifies which chain config to grab.
    #[arg(
        value_enum,
        long,
        short=None,
        value_name = "STRING",
        env = "CHAINDEXER_CHAIN_ID"
    )]
    pub chain: Chain,
    /// Store to use. Must be a valid key under the `stores` namespace in config file. If not
    /// supplied it will use a store conf with the same name as the chain.
    #[arg(long, short=None, value_name="STRING", env = "CHAINDEXER_MAP_STORE")]
    pub store_conf: Option<String>,
    /// Block to start on. If not set, starts on block 1
    ///
    /// If building an existing map, this will be be ignored, it will start with the
    /// beginning of the last partition written for each table (starts at beginning
    /// because it is unknown whether or not the data's been fully loaded.)
    #[arg(long, short=None, value_name="INT")]
    pub start_block: Option<u64>,
    /// When this block is seen, stop building the data map.
    /// Defaults to the most up-to-date/current block
    /// reported by the underlying chain: [`ChainDef::newest_block_num`]
    #[arg(long, short=None, value_name="INT")]
    pub end_block: Option<u64>,
    /// Specify the number of blocks each partition should have. Note that this arg
    /// will only be used in the case of creating a new data map.
    #[arg(long, short=None, value_name="INT")]
    pub blocks_per_partition: Option<u64>,
    /// Comma-separated list of tables to ignore building for.
    #[arg(long, short=None, value_name="STRING")]
    pub skip_tables: Option<String>,
    /// When fetching data, chunks of blocks are requested at a time and then those
    /// are put into arrow batches. This determines how many blocks (not rows) are in each of those batches.
    #[arg(long, short=None, value_name="INT", default_value="1000")]
    pub blocks_per_batch: u64,
    #[arg(long, short)]
    pub yes: bool,
}

impl BuildIndexCommand {
    pub async fn run(self, conf: GlobalConf) -> Result<ChainPartitionIndex, BuildChainErr> {
        // TODO: add confirmation if user might overwrite
        let chain_id = self
            .chain
            .to_possible_value()
            .ok_or_else(|| BuildChainErr::ArgError {
                arg: "chain-id".to_string(),
                message: "".to_string(),
            })?;
        let chain_id = chain_id.get_name();
        debug!("cli command build-chain-data using chain_id={chain_id}.");
        let chain_conf = conf
            .chains
            .get(chain_id)
            .ok_or_else(|| BuildChainErr::ArgError {
                arg: "chain-conf".to_owned(),
                message: format!("No config found for chain {}!", chain_id.cyan().bold()),
            })?;
        // try initialize chain using conf
        let chain =
            self.chain
                .try_init_empty(Some(chain_conf))
                .map_err(|err| BuildChainErr::ArgError {
                    arg: "chain".to_owned(),
                    message: format!(
                        "Config for chain {} is invalid: {}",
                        chain_id.cyan().bold(),
                        err.to_string().bold()
                    ),
                })?;
        let chain = Arc::from(chain) as Arc<dyn ChainApi>;
        info!("initialized chain: id={}", chain.name());
        let store_conf_name = self.store_conf.unwrap_or_else(|| chain_id.to_owned());
        // now check the rest of the args
        let store_conf =
            conf.stores
                .get(&store_conf_name)
                .ok_or_else(|| BuildChainErr::ArgError {
                    arg: "store".to_owned(),
                    message: format!(
                        "No store named {} found in config!",
                        store_conf_name.cyan().bold()
                    ),
                })?;
        info!("got store_conf: scheme={}", store_conf.scheme());
        let skip_tables = self
            .skip_tables
            .map(|ls| ls.split(", ").map(|s| s.trim().to_owned()).collect_vec());

        let opts = RunOpts {
            chain,
            store: store_conf.to_owned(),
            blocks_per_chunk: self.blocks_per_batch,
            start_block: self.start_block,
            end: self.end_block,
            skip: skip_tables,
            no_confirms: !self.yes,
            partition_size: self.blocks_per_partition,
            global_conf: Some(conf),
        };
        run_build_chaindata(opts).await
    }
}

// static mut OUTPUT: Mutex<TerminalOutput> = Mutex::new(TerminalOutput::init());

struct RunOpts {
    chain: Arc<dyn ChainApi>,
    store: StorageConf,
    blocks_per_chunk: u64,
    start_block: Option<u64>,
    end: Option<u64>,
    skip: Option<Vec<String>>,
    no_confirms: bool,
    partition_size: Option<u64>,
    /// global conf is set when this is run from cli
    global_conf: Option<GlobalConf>,
}

/// helper for generating a filename to store the db in the data dir
///
/// blocking since this should only be called towards the top of the cli command,
/// before processing actually starts.
fn db_filename(datadir: impl AsRef<Path>, chain_id: &str) -> PathBuf {
    for i in 0..99999 {
        let name = match i {
            0 => format!("{chain_id}.db"),
            _ => format!("{chain_id}_{i}.db"),
        };
        let filepath = PathBuf::from(datadir.as_ref()).join(name);
        if !filepath.exists() {
            return filepath;
        }
    }
    panic!("too many iterations when calculating filename. something went wrong!");
}

/// Given a chain, build its [`ChainDataIndex`]. This will load data using the chain, `T`,
/// using the [`StorageApi`] (created by `opts.store`) to save partitions.
///
/// The mapping is "checkpointed" by being persisted (using [`StorageApi::save`])
/// after partitions are generated.
async fn run_build_chaindata(opts: RunOpts) -> Result<ChainPartitionIndex, BuildChainErr> {
    // =============== setup ===============
    let RunOpts {
        chain,
        store,
        blocks_per_chunk,
        start_block,
        end,
        skip,
        no_confirms,
        partition_size,
        global_conf,
    } = opts;
    let store = StorageApi::<ChainPartitionIndex>::try_new(store)
        .await
        .map_err(BuildChainErr::SetupError)?;
    debug!("using {} store", store.scheme());
    let names_to_skip = skip.unwrap_or(vec![]);
    let chain_id = chain.name();
    let all_tables = Arc::clone(&chain).get_tables();
    let tables = all_tables
        .iter()
        .map(Arc::clone)
        .filter(|t| {
            let name = t.name().to_owned();
            let skip = names_to_skip.contains(&name);
            if skip {
                debug!("skipping table {name}");
            }
            !skip
        })
        .collect_vec();
    let table_names = tables.iter().map(|t| t.name().to_string()).collect_vec();
    // OUTPUT.set_tables(table_names.to_owned());
    let (mut chain_idx, start_block) = load_or_create_intial_index(
        &store,
        Arc::clone(&chain),
        all_tables.iter().map(Arc::clone).collect_vec(),
        start_block,
        partition_size,
        no_confirms,
    )
    .await?;
    // move data index from the tempfile into a permanent file in the datadir
    // supplied by user
    if let Some(c) = global_conf {
        let fname = db_filename(&c.app_data_dir, chain_id);
        chain_idx
            .db_set_local_disk_path(&fname)
            .await
            .map_err(BuildChainErr::SetupError)?;
    }
    // =============== main logic ===============
    println!(
        "starting chain {}... tables: {}",
        chain.name().cyan().bold(),
        table_names.iter().join(", ").cyan().bold()
    );
    let partsize = chain_idx.blocks_per_partition();
    let mut cur_block = start_block;
    loop {
        let mode =
            IterState::current_state(cur_block, end, partsize, Arc::clone(&chain), no_confirms)
                .await?;

        match mode {
            IterState::Exit => {
                return Ok(chain_idx);
            }
            IterState::Historic {
                start_iter,
                finish_iter,
                exit_after,
            } => {
                // build each table's partition concurrently
                let part_results =
                    futures::future::join_all(tables.iter().map(Arc::clone).map(|table| {
                        chain_idx.create_partition(
                            start_iter,
                            table,
                            &store,
                            CreatePartitionOpts {
                                end: Some(finish_iter),
                                blocks_per_batch: Some(blocks_per_chunk),
                                ..Default::default()
                            },
                        )
                    }))
                    .await;
                let mut block_parts = vec![];
                let mut errors = vec![];
                for (res, table) in part_results.into_iter().zip(table_names.iter()) {
                    match res {
                        Ok(block_part) => {
                            block_parts.push(block_part);
                        }
                        Err(e) => {
                            errors.push((table.to_string(), e));
                        }
                    }
                }
                if !errors.is_empty() {
                    // if any errors occurred for entities immediately return
                    // the successful partitions are included in the error enum in case
                    // we want to use those
                    return Err(BuildChainErr::IterErr {
                        errors,
                        complete: block_parts,
                        data: chain_idx.clone(),
                    });
                } else {
                    println!(
                        "{}",
                        "iteration complete! all tables successfully loaded. \
                         saving index..."
                            .green()
                    );
                    // persist the index after the iteration is complete
                    chain_idx
                        .save(&store)
                        .await
                        .map_err(|e| BuildChainErr::StorageApiError {
                            error: e.context("failed to save chain data mapping to storage."),
                            data: chain_idx.clone(),
                        })?;
                }
                if exit_after {
                    return Ok(chain_idx);
                } else {
                    cur_block += partsize;
                }
            }
            IterState::Live { .. } => todo!("still have to implement building live chain data"),
        }
    }
}

#[derive(Debug, Error)]
pub enum BuildChainErr {
    #[error("Got invalid value for '{arg}':  {message}")]
    ArgError { arg: String, message: String },
    #[error("Error occurred during setup ")]
    SetupError(#[from] anyhow::Error),
    #[error("Store holds existing map which is invalid")]
    InvalidExistingStore(String),
    /// error occurred during an iteration (primary part of the process).
    #[error("one or more tables failed to load/create partition data")]
    IterErr {
        /// list of entity name/error pairs
        errors: Vec<(String, anyhow::Error)>,
        /// list of successfully created partitions in the iteration that failed
        complete: Vec<BlockPartition>,
        /// the chain data map that was being built
        data: ChainPartitionIndex,
    },
    #[error(
        "after a successful iteration, while updating/persisting data mapping, \
         following error occurred: {error}"
    )]
    StorageApiError {
        error: anyhow::Error,
        data: ChainPartitionIndex,
    },
}
/// state of current iteration
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum IterState {
    /// if the currently being built partition contains the most recent block
    Live { start: u64 },
    /// if there is a specified endblock OR the currently being built partition
    /// does not include the most recent chain block.
    Historic {
        start_iter: u64,
        finish_iter: u64,
        /// set flag true if this is the last iteration before exit
        exit_after: bool,
    },
    /// return early
    Exit,
}
impl IterState {
    /// get the current state.
    async fn current_state(
        block: u64,
        end_block: Option<u64>,
        partition_size: u64,
        chain: Arc<dyn ChainApi>,
        no_confirms: bool,
    ) -> Result<Self, BuildChainErr> {
        let mode = match end_block {
            Some(end_block) => {
                if block + partition_size < end_block {
                    Self::Historic {
                        start_iter: block,
                        finish_iter: block + partition_size,
                        exit_after: false,
                    }
                } else {
                    let incomplete = block + partition_size > end_block;
                    if incomplete {
                        println!(
                            "{}",
                            format!(
                                "Final partition should be blocks {} thru {} \
                                 but specified end block is {}. \
                                 The partitions created this iteration will be incomplete.",
                                block.to_string().blue().bold(),
                                (block + partition_size).to_string().blue().bold(),
                                end_block.to_string().blue().bold(),
                            )
                            .yellow()
                        );
                        if !no_confirms
                            && !Confirm::with_theme(&ColorfulTheme::default())
                                .with_prompt("Continue with building partition?")
                                .interact()
                                .unwrap()
                        {
                            return Ok(Self::Exit);
                        }
                    }
                    Self::Historic {
                        start_iter: block,
                        finish_iter: end_block,
                        exit_after: true,
                    }
                }
            }
            None => {
                let maxblock = chain
                    .max_blocknum()
                    .await
                    .map_err(|e| e.context("failed to get current block"))
                    .map_err(BuildChainErr::SetupError)?;
                debug!("newest block for chain: {}", maxblock);
                if block + partition_size > maxblock {
                    Self::Live { start: block }
                } else {
                    // when block+partition_size is equal to maxblock, its not actually
                    // the last iter b/c max block will go up on the next iter.
                    // so in the rare case where max_block and block+partition_size
                    // are exactly equal, we can still use historic mode
                    Self::Historic {
                        start_iter: block,
                        finish_iter: block + partition_size,
                        exit_after: false,
                    }
                }
            }
        };
        Ok(mode)
    }
}

async fn get_start_block_for_map(map: &ChainPartitionIndex) -> Result<u64> {
    let partition_size = map.blocks_per_partition();
    let tables = map.table_names().await?;
    // last partition of every table
    let mut last_parts = vec![];
    for name in tables {
        let table = map
            .get_table(&name)
            .await?
            .expect("table should always exist since we are iterating over existing names");
        if let Some(part) = table.last_partition().await? {
            last_parts.push(part);
        }
    }
    // they should all have same lower bound but just in case take the smallest
    match last_parts.iter().min_by_key(|b| b.lower_bound) {
        None => Ok(0),
        Some(block) => {
            if block.incomplete {
                // if partition is incomplete the best thing to do is start over
                // at beginning
                Ok(block.lower_bound)
            } else {
                // start at next partition
                Ok(block.lower_bound + partition_size)
            }
        }
    }
}

/// helper method that either:
/// - loads an existing underlying partition index
/// - creates a new index
/// and in both cases it determines the block to write until
///
/// returns a tuple of:
/// 1. the (brand new or existing) index
/// 2. block to write until
async fn load_or_create_intial_index(
    store: &StorageApi<ChainPartitionIndex>,
    chain_ref: Arc<dyn ChainApi>,
    table_refs: Vec<Arc<dyn TableApi>>,
    start_block: Option<u64>,
    partition_size: Option<u64>,
    no_confirms: bool,
) -> Result<(ChainPartitionIndex, u64), BuildChainErr> {
    let chain_name = chain_ref.name();
    match store.load().await.map_err(BuildChainErr::SetupError)? {
        Some(existing_map) => {
            // mapping store already contained data
            info!("found existing index");
            let cur_chain = chain_name.to_owned();
            let store_chain = existing_map.chain_id();
            if cur_chain != store_chain {
                let msg = format!(
                    "Trying to build partition index for chain '{}' but store holds an existing \
                     index for chain '{}'. \
                     Use a new store or delete the existing data in this store.",
                    cur_chain.cyan().bold(),
                    store_chain.cyan().bold()
                );
                println!("{msg}");
                return Err(BuildChainErr::InvalidExistingStore(msg));
            }
            if let Some(ps) = partition_size {
                println!(
                    "{}",
                    format!(
                        "blocks-per-partition of {} was given but \
                        partition index already existed. Will be ignored...",
                        ps.to_string().blue().bold()
                    )
                    .yellow()
                    .bold()
                );
            }
            // NB: all the tables should be on the same block but just in case we can
            // just take whatever the lowest start block is between all tables
            let startblock = get_start_block_for_map(&existing_map).await?;
            println!(
                "{}",
                format!(
                    "Start block: {} (computed from existing index)",
                    startblock.to_string().blue().bold()
                )
                .cyan()
            );
            Ok((existing_map, startblock))
        }
        None => {
            info!("no existing index found. preparing to initialize a new one");
            println!(
                "{}",
                "Nothing existed in store... initializing empty one.".green()
            );
            let startblock = match start_block {
                Some(s) => {
                    println!(
                        "{}",
                        format!("Start block: {} (from args)", s.to_string().blue().bold()).cyan()
                    );
                    s
                }
                None => {
                    println!(
                        "{}",
                        format!("Starting from block: {} (default)", "0".blue().bold()).cyan()
                    );
                    0
                }
            };
            let defaultsize = Arc::clone(&chain_ref).blocks_per_part();
            let part_size = match (partition_size, no_confirms) {
                (None, true) => {
                    // no partition size supplied, use interactive to determine
                    match Confirm::with_theme(&ColorfulTheme::default())
                        .with_prompt(format!(
                            "Use suggested blocks per partition, {}, for {}?",
                            defaultsize.to_string().blue().bold(),
                            chain_name.cyan().bold()
                        ))
                        .interact()
                        .unwrap()
                    {
                        true => defaultsize,
                        false => Input::with_theme(&ColorfulTheme::default())
                            .with_prompt("Enter partition size: ")
                            .interact_text()
                            .unwrap(),
                    }
                }
                (None, false) => {
                    // no partition size supplied, use default for chain
                    println!(
                        "Using default partition size {} for chain {}",
                        defaultsize.to_string().blue().bold(),
                        chain_name.to_owned().cyan().bold()
                    );
                    defaultsize
                }
                (Some(ps), _) => ps,
            };
            let new_map =
                ChainPartitionIndex::try_new(Arc::clone(&chain_ref).name(), part_size).await?;
            // define all tables, even ones being skipped
            for t in table_refs {
                new_map.register_table(t.clone()).await?;
            }
            Ok((new_map, startblock))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        chains::eth::{
            test::{get_rpc_url, start_block},
            EthChain, EthDynConf,
        },
        chains::test::{empty_url, ErrorChain, TestChain},
        chains::{ChainConf, ChainDef},
        storage::ipfs::tests::{ipfs_integration_test, ipfs_store},
        test::{integration_test_flag, setup_integration, TestDir},
        util::RpcApiConfig,
    };
    use chrono::Utc;
    use once_cell::sync::Lazy;
    static CHAIN: Lazy<Arc<dyn ChainApi>> = Lazy::new(|| {
        Arc::new(TestChain::new(ChainConf {
            partition_index: None,
            data_fetch_conf: Some(()),
        }))
    });
    macro_rules! buildopts {
        ($store: ident, $start: literal, $end: literal, $part_size: literal) => {
            RunOpts {
                chain: CHAIN.clone(),
                blocks_per_chunk: 10,
                start_block: Some($start),
                end: Some($end),
                no_confirms: true,
                skip: Some(vec![]),
                store: $store.conf().clone(),
                partition_size: Some($part_size),
                global_conf: None,
            }
        };
    }
    macro_rules! genstore {
        ($dir: ident) => {
            StorageApi::<ChainPartitionIndex>::try_new(StorageConf::File {
                dirpath: $dir.path.clone(),
                filename: "testy.json".to_string(),
            })
            .await
            .unwrap()
        };
    }
    #[tokio::test]
    async fn build_new_chain_index_one_iter() {
        let dir = TestDir::new(true);
        let store = genstore!(dir);
        let built = run_build_chaindata(buildopts!(store, 0, 100, 100))
            .await
            .unwrap();
        let table_name = built
            .table_names()
            .await
            .unwrap()
            .get(0)
            .unwrap()
            .to_owned();
        let table = built.get_table(&table_name).await.unwrap().unwrap();
        let blockpart = table.get_partition(0).await.unwrap().unwrap();
        assert_eq!(blockpart.row_count, Some(100));
    }
    async fn emptymap(part_size: u64) -> ChainPartitionIndex {
        let new_map = ChainPartitionIndex::try_new(CHAIN.name(), part_size)
            .await
            .unwrap();
        for t in CHAIN.clone().get_tables() {
            new_map.new_table(t.name(), t.blocknum_col()).await.unwrap();
        }
        new_map
    }

    #[tokio::test]
    async fn build_chain_index_existing_empty_map() {
        let dir = TestDir::new(true);
        let empty_map = emptymap(10).await;
        empty_map.add_metadata("test", "flag").await.unwrap();
        let store_conf = StorageConf::File {
            dirpath: dir.path.clone(),
            filename: "testy.json".to_string(),
        };
        let store = StorageApi::<ChainPartitionIndex>::try_new(store_conf.clone())
            .await
            .unwrap();
        store.save(&empty_map).await.unwrap();
        let built = run_build_chaindata(buildopts!(store, 0, 10, 100))
            .await
            .unwrap();
        // make sure it loaded the existing map!
        let mdata = built.get_metadata().await.unwrap();
        assert_eq!(mdata.get("test").unwrap(), "flag");
    }
    #[tokio::test]
    async fn build_new_chain_index_existing_nonempty() {
        let dir = TestDir::new(true);
        let index = emptymap(10).await;
        let tablenames = index.table_names().await.unwrap();
        let tablename = tablenames.get(0).unwrap();
        let tablecount = tablenames.len();
        for t in &tablenames {
            index
                .add_partition(BlockPartition {
                    table: t.to_owned(),
                    lower_bound: 0,
                    location: empty_url(),
                    byte_count: Some(10),
                    row_count: Some(10),
                    incomplete: false,
                    created_at: Utc::now(),
                })
                .await
                .unwrap();
        }
        assert!(index.get_partition(tablename, 10).await.unwrap().is_none());
        let store = genstore!(dir);
        store.save(&index).await.unwrap();
        // start and partition size should be ignored
        let built = run_build_chaindata(buildopts!(store, 0, 20, 0))
            .await
            .unwrap();
        let newcount = built.partition_count().await.unwrap();
        assert_eq!(newcount, tablecount * 2);
        assert!(built.get_partition(tablename, 10).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn build_new_chain_index_existing_nonempty_starts_above_zero() {
        let dir = TestDir::new(true);
        let empty_map = emptymap(10).await;
        let tablenames = empty_map.table_names().await.unwrap();
        let tablename = tablenames[0].to_owned();
        for t in tablenames {
            empty_map
                .add_partition(BlockPartition {
                    table: t,
                    lower_bound: 100,
                    location: empty_url(),
                    byte_count: Some(10),
                    row_count: Some(10),
                    incomplete: false,
                    created_at: Utc::now(),
                })
                .await
                .unwrap();
        }
        let store = genstore!(dir);
        store.save(&empty_map).await.unwrap();
        // start and partition size should be ignored!
        let built = run_build_chaindata(buildopts!(store, 999, 120, 999))
            .await
            .unwrap();
        let table = built.get_table(&tablename).await.unwrap().unwrap();
        let partition = table.last_partition().await.unwrap().unwrap();
        assert_eq!(partition.row_count, Some(10));
        assert_eq!(partition.lower_bound, 110);
    }

    #[tokio::test]
    async fn build_new_chain_index_fetching_err() {
        let dir = TestDir::new(true);
        let opts = RunOpts {
            chain: Arc::new(ErrorChain::new(ChainConf {
                partition_index: None,
                data_fetch_conf: Some(()),
            })),
            blocks_per_chunk: 10,
            start_block: None,
            end: Some(100),
            no_confirms: true,
            global_conf: None,
            skip: Some(vec![]),
            store: StorageConf::File {
                dirpath: dir.path.clone(),
                filename: "testy.json".to_string(),
            },
            partition_size: Some(1000),
        };
        let built_err = run_build_chaindata(opts).await.unwrap_err();
        match built_err {
            BuildChainErr::IterErr {
                errors,
                complete,
                data,
            } => {
                assert_eq!(errors.len(), 1);
                assert_eq!(complete.len(), 1);
                assert_eq!(data.table_names().await.unwrap().len(), 2);
                let mut has_none = 0;
                for t in data.all_tables().await.unwrap() {
                    if t.last_partition().await.unwrap().is_none() {
                        has_none += 1;
                    }
                }
                // the chain that errored should have no partitions
                assert_eq!(has_none, 1);
            }
            _ => panic!("unexpected error variant: {built_err:?}"),
        }
    }

    #[tokio::test]
    async fn test_get_start_block() {
        let dm = ChainPartitionIndex::try_new("test", 10).await.unwrap();
        let t1 = "table1";
        let t2 = "table2";
        let byte_count = Some(1);
        let url = empty_url();
        let row_count = Some(10);
        dm.new_table(t1, "c").await.unwrap();
        dm.new_table(t2, "c").await.unwrap();
        let names = dm.table_names().await.unwrap();
        for table in names.iter().cloned() {
            dm.add_partition(BlockPartition {
                table,
                lower_bound: 0,
                incomplete: false,
                location: url.to_owned(),
                byte_count,
                row_count,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        }
        let sblock = get_start_block_for_map(&dm).await.unwrap();
        assert_eq!(10, sblock);
        for table in names.iter().cloned() {
            dm.add_partition(BlockPartition {
                table,
                lower_bound: 10,
                incomplete: true,
                location: url.to_owned(),
                byte_count,
                row_count,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        }
        let sblock = get_start_block_for_map(&dm).await.unwrap();
        assert_eq!(10, sblock);
        // only one table has last partition completed now
        dm.add_partition(BlockPartition {
            table: t1.to_string(),
            lower_bound: 10,
            location: url.to_owned(),
            byte_count,
            row_count,
            incomplete: false,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
        // should start with the incomplete one
        assert_eq!(10, sblock);
    }

    // ======================== integration tests below ========================
    // if TEST_INTEGRATION is not set in env, these get skipped.

    /// if integraion tests are on, setup an eth chain for testing against an RPC
    macro_rules! integration_eth {
        () => {{
            if !integration_test_flag() {
                eprintln!("skipping integration test...");
                return;
            }
            setup_integration();
            let required_vars = vec!["TEST_ETH_RPC_URL"];
            for v in required_vars {
                if std::env::var(v).is_err() {
                    panic!("reuqired environment var {v} not found!");
                }
            }
            let c = Arc::new(EthChain::new(ChainConf {
                partition_index: None,
                data_fetch_conf: Some(EthDynConf {
                    rpc: RpcApiConfig {
                        url: Some(get_rpc_url()),
                        batch_size: Some(250),
                        request_timeout_ms: Some(60_000),
                        ..Default::default()
                    },
                }),
            }));
            c as Arc<dyn ChainApi>
        }};
    }
    #[tokio::test]
    async fn integration_test_build_chaindata_eth() {
        let chain = integration_eth!();
        let block = start_block();
        let dir = TestDir::new(true);
        let partition_size = Some(25);
        let row_count = 50;
        let opts = RunOpts {
            chain,
            blocks_per_chunk: 5,
            global_conf: None,
            start_block: Some(block),
            end: Some(block + row_count),
            no_confirms: true,
            skip: Some(vec![]),
            store: StorageConf::File {
                dirpath: dir.path.clone(),
                filename: "testy.json".to_string(),
            },
            partition_size,
        };
        let built_map = run_build_chaindata(opts).await.unwrap();
        let built_blocks = built_map.get_table("blocks").await.unwrap().unwrap();
        let mut got_row_count = 0;
        for id in built_blocks.partition_ids().await.unwrap() {
            let part = built_blocks.get_partition(id).await.unwrap().unwrap();
            got_row_count += part.row_count.unwrap();
        }
        assert_eq!(got_row_count, row_count);
    }

    #[tokio::test]
    async fn integration_test_build_chaindata_ipfs_store() -> anyhow::Result<()> {
        ipfs_integration_test!();
        let ipfs_store = ipfs_store().await;
        let store = StorageApi::<ChainPartitionIndex>::try_new(StorageConf::Ipfs(ipfs_store.conf))
            .await
            .unwrap();
        let built = run_build_chaindata(buildopts!(store, 0, 100, 100))
            .await
            .unwrap();
        let blockpart = built
            .all_tables()
            .await?
            .into_iter()
            .next()
            .unwrap()
            .get_partition(0)
            .await?
            .unwrap();
        assert_eq!(blockpart.location.scheme(), "ipfs");
        Ok(())
    }
}
