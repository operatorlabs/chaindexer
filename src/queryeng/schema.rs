//! Datafusion catalogs/schemas discovery for a chain

use std::sync::Arc;

use crate::{chains::ChainApi, table_api::TableApi};
use anyhow::anyhow;
use async_trait::async_trait;
use colored::Colorize;
use dashmap::DashMap;
use datafusion::{
    catalog::{
        catalog::{CatalogList, CatalogProvider},
        schema::SchemaProvider,
    },
    datasource::TableProvider,
};
use itertools::Itertools;
use log::error;

use super::{ctx::CtxStateRef, provider::ChaindexerTableProvider};

/// Global catalog list used by context. Right now there is only one catalog.
pub struct GlobalCatalogs {
    catalog: Arc<Catalog>,
}
impl GlobalCatalogs {
    pub fn new(state: CtxStateRef) -> Self {
        Self {
            catalog: Arc::new(Catalog::new(state)),
        }
    }
    /// Get the main (currently only) catalog...
    pub fn catalog(&self) -> Arc<Catalog> {
        self.catalog.clone()
    }
}
pub const DEFAULT_CATALOG: &str = "main";
impl CatalogList for GlobalCatalogs {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unreachable!()
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![DEFAULT_CATALOG.to_owned()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == DEFAULT_CATALOG {
            Some(self.catalog.clone())
        } else {
            None
        }
    }
}
pub struct Catalog {
    chains: DashMap<String, Arc<ChainSchema>>,
    state: CtxStateRef,
}
impl Catalog {
    pub fn new(state: CtxStateRef) -> Self {
        Self {
            chains: DashMap::new(),
            state,
        }
    }
    pub fn register_chain(&self, chain: Arc<dyn ChainApi>) {
        let schema = ChainSchema::new(chain.clone(), self.state.clone());
        self.chains
            .insert(chain.name().to_owned(), Arc::new(schema));
    }
    /// Like register chain but also loads all the table providers (requires async schema
    /// resolution.)
    pub async fn register_chain_table_providers(
        &self,
        chain: Arc<dyn ChainApi>,
    ) -> anyhow::Result<()> {
        let schema = ChainSchema::new(chain.clone(), self.state.clone());
        for t in schema.table_names() {
            schema
                .table(&t)
                .await
                .ok_or_else(|| anyhow!("failed to load table"))?;
        }
        Ok(())
    }

    pub fn get_chain(&self, name: &str) -> Option<Arc<ChainSchema>> {
        self.chains.get(name).map(|kv| kv.value().clone())
    }
}
impl CatalogProvider for Catalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.chains
            .iter()
            .map(|c| c.chain.name().to_owned())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.chains
            .get(name)
            .map(|kv| kv.value().clone() as Arc<dyn SchemaProvider>)
    }
}

pub struct ChainSchema {
    chain: Arc<dyn ChainApi>,
    tables: DashMap<String, Arc<dyn TableApi>>,
    /// lazily instantiated table provider objects
    providers: DashMap<String, Arc<ChaindexerTableProvider>>,
    state: CtxStateRef,
}
impl ChainSchema {
    pub fn new(chain: Arc<dyn ChainApi>, state: CtxStateRef) -> Self {
        let tables = chain
            .clone()
            .get_tables()
            .into_iter()
            .fold(DashMap::new(), |map, table| {
                map.insert(table.name().to_owned(), table);
                map
            });
        Self {
            chain,
            tables,
            providers: DashMap::new(),
            state,
        }
    }
    /// Get [`TableApi`] object in this chain with name `table_name`
    pub fn table_api(&self, table_name: &str) -> Option<Arc<dyn TableApi>> {
        self.tables.get(table_name).map(|kv| kv.value().clone())
    }
}
#[async_trait]
impl SchemaProvider for ChainSchema {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|kv| kv.key().to_string())
            .collect_vec()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        if let Some(existing) = self.providers.get(name) {
            Some(existing.clone())
        } else {
            let table = self.tables.get(name);
            if let Some(kv) = table {
                let table = kv.value().clone();
                match ChaindexerTableProvider::try_create(table, self.state.clone()).await {
                    Ok(provider) => {
                        let provider = Arc::new(provider);
                        self.providers.insert(name.to_owned(), provider.clone());
                        Some(provider)
                    }
                    Err(err) => {
                        error!("failed to init table {name} due to {err}");
                        println!(
                            "{}",
                            format!("failed to initialize table \"{}\" !", name.cyan())
                                .bold()
                                .red()
                        );
                        None
                    }
                }
            } else {
                None
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
