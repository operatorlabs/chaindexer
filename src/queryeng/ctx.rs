use crate::chains::ChainApi;
use crate::partition_index::ChainPartitionIndex;
use crate::queryeng::provider::ChaindexerTableProvider;
use crate::storage::Location;
use crate::storage::{Persistable, StorageApi, StorageConf};
use crate::table_api::TableApi;
use anyhow::{bail, Result};
use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use log::{debug, info, warn};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::schema::{Catalog, GlobalCatalogs};

pub struct Ctx {
    // custom state
    state: Arc<CtxState>,
    df_ctx: RwLock<SessionContext>,
    global_catalogs: Arc<GlobalCatalogs>,
}
impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx").field("state", &self.state).finish()
    }
}
pub type CtxStateRef = Arc<CtxState>;
impl Ctx {
    pub fn new() -> Self {
        let state: CtxState = Default::default();
        let state = Arc::new(state);
        let global_catalogs = Arc::new(GlobalCatalogs::new(state.clone()));
        let mut df_ctx = SessionContext::new();
        df_ctx.register_catalog_list(global_catalogs.clone());
        Self {
            df_ctx: RwLock::new(df_ctx),
            state,
            global_catalogs,
        }
    }
    pub fn catalog(&self) -> Arc<Catalog> {
        self.global_catalogs.catalog()
    }
    /// Get a read-only lock to the underlying datafusion context
    pub fn ctx(&self) -> RwLockReadGuard<SessionContext> {
        self.df_ctx.read()
    }
    /// Get a mutable lock to the underlying datafusion context
    pub fn ctx_mut(&self) -> RwLockWriteGuard<SessionContext> {
        self.df_ctx.write()
    }

    // pub fn into_owned_ctx() ->

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
        self.catalog().register_chain(chain);
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
    /// all storage confs
    store_confs: RwLock<HashMap<String, StorageConf>>,
    /// instantiated stores for chain indices
    chain_idx_stores: StorgeApiMap<ChainPartitionIndex>,
}

impl Default for CtxState {
    fn default() -> Self {
        Self {
            blocks_per_batch: 100,
            start_block: None,
            end_block: None,
            store_confs: RwLock::new(HashMap::new()),
            chain_idx_stores: StorgeApiMap::new(),
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
    use crate::storage::ObjStorePath;
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
