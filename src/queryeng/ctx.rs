use crate::chains::ChainApi;
use crate::partition_index::ChainPartitionIndex;
use crate::storage::Location;
use crate::storage::{Persistable, StorageApi, StorageConf};
use crate::table_api::TableApi;

use anyhow::{bail, Result};
use colored::Colorize;
use datafusion::arrow::array::{as_string_array, ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::Volatility;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::prelude::{create_udf, SessionConfig, SessionContext};
use itertools::Itertools;
use log::{debug, warn};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::schema::{Catalog, GlobalCatalogs, DEFAULT_CATALOG};

pub struct Ctx {
    // custom state
    state: Arc<CtxState>,
    df_ctx: SessionContext,
    global_catalogs: Arc<GlobalCatalogs>,
}
impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx").field("state", &self.state).finish()
    }
}
impl Default for Ctx {
    fn default() -> Self {
        Self::new()
    }
}
pub type CtxStateRef = Arc<CtxState>;
// b/c of the way datafusion udfs work (requires 'static refs so that it can be `Fn`),
// we can only save the current block number for the duration
// of the program lifetime once its loaded.
// TODO: im sure there's a better way to do this
// static CURRENT_BLOCKS: OnceCell<HashMap<String, u64>> = OnceCell::new();
impl Ctx {
    pub fn new() -> Self {
        let state: CtxState = Default::default();
        let state = Arc::new(state);
        let global_catalogs = Arc::new(GlobalCatalogs::new(state.clone()));
        // TODO: expose other datafusion opts here
        let conf = SessionConfig::default()
            .with_information_schema(true)
            .with_default_catalog_and_schema(DEFAULT_CATALOG, "eth");
        let mut df_ctx = SessionContext::with_config(conf);
        df_ctx.register_catalog_list(global_catalogs.clone());
        Self {
            df_ctx,
            state,
            global_catalogs,
        }
    }
    pub fn catalog(&self) -> Arc<Catalog> {
        self.global_catalogs.catalog()
    }
    /// Get a ref to the underlying datafusion context
    pub fn ctx(&self) -> &SessionContext {
        &self.df_ctx
    }
    /// Get a mutable lock to the underlying datafusion context
    pub fn ctx_mut(&mut self) -> &mut SessionContext {
        &mut self.df_ctx
    }
    /// initializes udfs for this context.
    pub async fn init_udfs(&self) {
        // use static mapping so that the udfs impl trait `Fn`
        static CUR_BLOCKS: OnceCell<RwLock<HashMap<String, u64>>> = OnceCell::new();
        let mut current_blocks = HashMap::new();
        for c in self.state.chains() {
            let current_block = c.max_blocknum().await;
            if let Ok(b) = current_block {
                current_blocks.insert(c.name().to_owned(), b);
            } else {
                println!(
                    "{}",
                    format!("failed to fetch current block for {}", c.name().cyan()).yellow()
                );
            }
        }
        if let Some(rwlock) = CUR_BLOCKS.get() {
            let mut wr = rwlock.write();
            for (k, v) in current_blocks.iter() {
                wr.insert(k.to_owned(), *v);
            }
        } else {
            CUR_BLOCKS.get_or_init(|| RwLock::new(current_blocks));
        }
        let current_block_impl = |args: &[ArrayRef]| {
            // this is guaranteed by DataFusion based on the function's signature.
            assert_eq!(args.len(), 1);
            let names = as_string_array(&args[0]);
            let blocks = CUR_BLOCKS.get().unwrap();
            let blocks = blocks.read();
            // 2. perform the computation
            let array = names
                .iter()
                .map(|v| match v {
                    Some(n) => blocks.get(n).copied(),
                    None => None,
                })
                .collect::<UInt64Array>();
            Ok(Arc::new(array) as ArrayRef)
        };

        let cur_block_udf = create_udf(
            "current_block",
            // expects two f64
            vec![DataType::Utf8],
            // returns f64
            Arc::new(DataType::UInt64),
            Volatility::Immutable,
            make_scalar_function(current_block_impl),
        );
        self.df_ctx.register_udf(cur_block_udf);
    }

    /// Get custom (i.e. non DataFusion) state
    pub fn state(&self) -> Arc<CtxState> {
        self.state.clone()
    }

    /// Add a storage config to the context. Storage configs can be used at execution time
    /// to load partition indices. the store is also registered into the datafusion
    /// context.
    ///
    /// `name` is just the key in the config mapping.
    ///
    /// this will fail if the storage config is invalid.
    pub async fn add_storage_conf(&self, name: &str, conf: &StorageConf) -> Result<()> {
        self.state().add_store(name, conf);
        let ctx = self.ctx();
        ctx.runtime_env().register_object_store(
            conf.scheme(),
            conf.bucket().unwrap_or(""),
            self.state.chain_idx_stores.get_store_api(conf).await?,
        );
        Ok(())
    }

    pub fn register_chain(&self, chain: Arc<dyn ChainApi>) {
        self.catalog().register_chain(chain.clone());
        self.state.add_chain(chain);
    }

    /// Given a `loc`, get a [`StorageApi`] that supports it, if one exists.
    /// If multiple exist, the first one seen is returned.
    pub async fn chain_store_for_loc(
        &self,
        loc: &Location,
    ) -> Result<Arc<StorageApi<ChainPartitionIndex>>> {
        self.state().chain_store_for_loc(loc).await
    }
}
type RegChain = (Arc<dyn ChainApi>, Vec<Arc<dyn TableApi>>);

/// custom state
#[derive(Debug)]
pub struct CtxState {
    /// target number of blocks of data in arrow record batches.
    blocks_per_batch: u64,
    /// if this is set, data before this block (inclusive) will implicitly be filtered out
    start_block: Option<u64>,
    /// if this is set, data after this block (exclusive) will implicitly be filtered out
    end_block: Option<u64>,
    /// if this is set, treat `end_block` as the current blocknumber (according to the RPC api),
    /// and `start_block` as `end_block - last_n`
    last_n: Option<u64>,
    /// all storage confs
    store_confs: RwLock<HashMap<String, StorageConf>>,
    /// instantiated stores for chain indices
    chain_idx_stores: StorgeApiMap<ChainPartitionIndex>,
    regd_chains: RwLock<Vec<Arc<dyn ChainApi>>>,
}

impl Default for CtxState {
    fn default() -> Self {
        Self {
            blocks_per_batch: 100,
            start_block: None,
            end_block: None,
            last_n: None,
            store_confs: RwLock::new(HashMap::new()),
            chain_idx_stores: StorgeApiMap::new(),
            regd_chains: RwLock::new(vec![]),
        }
    }
}

impl CtxState {
    pub fn add_store(&self, name: &str, conf: &StorageConf) {
        self.store_confs
            .write()
            .insert(name.to_owned(), conf.to_owned());
    }
    /// Given a `loc`, get a [`StorageApi`] for chain indices that supports it, if one exists.
    /// If multiple exist, the first one seen is returned.
    pub async fn chain_store_for_loc(
        &self,
        loc: &Location,
    ) -> Result<Arc<StorageApi<ChainPartitionIndex>>> {
        let conf = {
            // acquire and drop readlock before await point
            let all_stores = self.store_confs.read();
            let mut valid_stores = all_stores.iter().filter(|(_, v)| v.location_is_valid(loc));
            if let Some((name, conf)) = valid_stores.next() {
                if valid_stores.next().is_some() {
                    warn!("detected multiple valid stores for '{loc}'. using store '{name}'",);
                }
                debug!("using storage conf '{name}' for {loc}");
                conf.to_owned()
            } else {
                bail!("no valid configs found")
            }
        };
        self.chain_idx_stores.get_store_api(&conf).await
    }

    pub async fn get_chain_idx_store(
        &self,
        conf: &StorageConf,
    ) -> Result<Arc<StorageApi<ChainPartitionIndex>>> {
        self.chain_idx_stores.get_store_api(conf).await
    }

    pub fn blocks_per_batch(&self) -> u64 {
        self.blocks_per_batch
    }
    pub fn start_block(&self) -> Option<u64> {
        self.start_block
    }
    pub fn end_block(&self) -> Option<u64> {
        self.end_block
    }
    fn add_chain(&self, c: Arc<dyn ChainApi>) {
        let mut wr = self.regd_chains.write();
        wr.push(c);
    }
    fn chains(&self) -> Vec<Arc<dyn ChainApi>> {
        let rdr = self.regd_chains.read();
        rdr.iter().cloned().collect_vec()
    }
}

/// Simple data stucture wrapping a mutex hashmap for storing
/// instantiated storage apis across logical/physical nodes.
#[derive(Debug)]
struct StorgeApiMap<T>
where
    T: Send + Sync + Persistable + 'static,
{
    data: Mutex<HashMap<StorageConf, Arc<StorageApi<T>>>>,
}
impl<T> StorgeApiMap<T>
where
    T: Send + Sync + Persistable + 'static,
{
    fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
    /// Given a storage conf, return a storage api for it.
    /// If it already existed, return that one, otherwise create a new one.
    /// If the config is invalid (storage api unable to be initialized), then return error.
    async fn get_store_api(&self, conf: &StorageConf) -> Result<Arc<StorageApi<T>>> {
        let mut map = self.data.lock().await;
        if let Some(existing) = map.get(conf) {
            Ok(existing.clone())
        } else {
            let store = Arc::new(StorageApi::try_new(conf.clone()).await.map_err(|e| {
                e.context(
                    "StoreMap got invalid storage conf! \
                     (failed to create store within get_store_api)",
                )
            })?);
            map.insert(conf.clone(), Arc::clone(&store));
            Ok(store)
        }
    }
    async fn len(&self) -> usize {
        self.data.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        chains::{
            test::{empty_chain, ErrorChain},
            ChainDef,
        },
        storage::ObjStorePath,
    };
    use datafusion::common::cast::as_uint64_array;
    use itertools::Itertools;
    use std::path::PathBuf;

    use super::*;
    use crate::{storage::Location, test::TestDir};
    fn testconfs(datadir: PathBuf) -> Vec<StorageConf> {
        vec![
            StorageConf::File {
                dirpath: datadir,
                filename: "testy.db".to_string(),
            },
            StorageConf::Memory {
                bucket: "bucket".to_owned(),
            },
            StorageConf::Memory {
                bucket: "bucket".to_owned(),
            },
        ]
    }

    async fn ctx_with_stores(tdir: PathBuf) -> Ctx {
        let (conf1, conf3, conf4) = testconfs(tdir).into_iter().collect_tuple().unwrap();
        let badconf = StorageConf::File {
            dirpath: PathBuf::from("/wtf"),
            filename: "testy.db".to_string(),
        };
        let ctx = Ctx::new();
        ctx.add_storage_conf("n1", &conf1).await.unwrap();
        ctx.add_storage_conf("n2", &badconf).await.unwrap_err();
        ctx.add_storage_conf("n3", &conf3).await.unwrap();
        ctx.add_storage_conf("n4", &conf4).await.unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_ctx_with_stores() {
        let dir = TestDir::new(true);
        let ctx = ctx_with_stores(dir.path.clone()).await;
        let state = ctx.state();
        let stores = state.store_confs.read();
        assert_eq!(stores.len(), 4);
        assert_eq!(stores.get("n1").unwrap().scheme(), "file");
    }
    #[tokio::test]
    async fn test_init_udfs() {
        let ctx = Ctx::new();
        let chain = empty_chain();
        let name = chain.name();
        ctx.register_chain(chain.clone());
        ctx.init_udfs().await;
        async fn get_block_for(chain: &str, ctx: &Ctx) -> Option<u64> {
            let q = format!("select current_block('{chain}')");
            let df = ctx.df_ctx.sql(&q).await.unwrap();
            let b = df.collect().await.unwrap();
            let batch = &b[0];
            let col = as_uint64_array(batch.column(0)).unwrap();
            col.iter().next().unwrap()
        }
        let res = get_block_for(name, &ctx).await;
        assert!(res.is_some());
        let res2 = get_block_for(ErrorChain::ID, &ctx).await;
        assert!(res2.is_none());
        // reg new chain but dont initialize udfs
        let errchain = Arc::new(ErrorChain::init().await) as Arc<dyn ChainApi>;
        ctx.register_chain(Arc::clone(&errchain));
        let res3 = get_block_for(ErrorChain::ID, &ctx).await;
        assert!(res3.is_none());
        // init udfs now and grab block num
        ctx.init_udfs().await;
        let res4 = get_block_for(ErrorChain::ID, &ctx).await;
        assert!(res4.is_some());
    }

    #[tokio::test]
    async fn test_ctx_with_stores_datafusion_registry() {
        let dir = TestDir::new(true);
        let confs = testconfs(dir.path.clone());
        let ctx = ctx_with_stores(dir.path.clone()).await;
        let conf = &confs[0];
        let loc = Location::new(
            conf.scheme(),
            conf.bucket(),
            ObjStorePath::from_absolute_path(dir.path.join("testy.db")).unwrap(),
        );
        let _objstore = ctx
            .ctx()
            .runtime_env()
            .object_store_registry
            .get_by_url(loc);
    }

    #[tokio::test]
    async fn test_store_map() {
        let dir = TestDir::new(true);
        let m = StorgeApiMap::<ChainPartitionIndex>::new();
        let (goodconf, match1, match2) = testconfs(dir.path.clone())
            .into_iter()
            .collect_tuple()
            .unwrap();
        let badconf = StorageConf::File {
            dirpath: PathBuf::from("/wtf"),
            filename: "testy.db".to_string(),
        };
        let api1 = m.get_store_api(&goodconf).await.unwrap();
        let api2 = m.get_store_api(&goodconf).await.unwrap();
        assert!(Arc::ptr_eq(&api1, &api2));
        assert_eq!(m.len().await, 1);
        m.get_store_api(&badconf).await.unwrap_err();
        assert_eq!(m.len().await, 1);
        let mem1 = m.get_store_api(&match1).await.unwrap();
        let mem2 = m.get_store_api(&match2).await.unwrap();
        assert_eq!(m.len().await, 2);
        assert!(Arc::ptr_eq(&mem1, &mem2));
    }

    #[tokio::test]
    async fn test_chain_idx_store() {
        let dir = TestDir::new(true);
        let ctx = ctx_with_stores(dir.path.clone()).await;
        let store = ctx
            .chain_store_for_loc(&Location::new(
                "file",
                None,
                ObjStorePath::from_absolute_path(dir.path.clone().join("ledata/lefile")).unwrap(),
            ))
            .await
            .unwrap();
        dbg!(&store);
        match store.conf() {
            StorageConf::File { dirpath, .. } => {
                assert_eq!(dirpath.to_owned(), dir.path.to_owned());
            }
            _ => panic!(),
        }
        let memstore = ctx
            .chain_store_for_loc(&Location::new(
                "memory",
                Some("bucket"),
                ObjStorePath::parse("/var/data/file").unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(
            std::mem::discriminant(memstore.conf()),
            std::mem::discriminant(&StorageConf::Memory {
                bucket: "testy".to_owned()
            })
        );
    }
}
